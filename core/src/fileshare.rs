use color_eyre::eyre::{bail, ensure, eyre};
use iroh::{Endpoint, EndpointAddr, endpoint::Connection, protocol::ProtocolHandler};
use iroh_blobs::{
    BlobsProtocol,
    format::collection::Collection,
    get::request::{get_hash_seq_and_sizes, get_verified_size},
    protocol::{ChunkRanges, GetRequest},
    ticket::BlobTicket,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::Cursor, sync::Arc};
use tokio::sync::{RwLock, oneshot};

pub const ALPN: &[u8] = b"/airdroppe/fileshare/1.0.0";
pub const HANDSHAKE: &[u8] = b"i cant be all about like, spelling and sh*t";

const MSG_LEN: usize = 2;
const MSG_NO: &[u8] = b"no";
const MSG_OK: &[u8] = b"ok";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ShareType {
    File,
    Files,
    Text,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Share {
    ticket: String,
    pub share: ShareType,
}

impl Share {
    pub fn new(ticket: BlobTicket, share: ShareType) -> Self {
        Self {
            ticket: ticket.to_string(),
            share,
        }
    }

    pub fn ticket(&self) -> Option<BlobTicket> {
        self.ticket.parse().ok()
    }
}

#[derive(Debug, Clone)]
pub struct Fileshare {
    endpoint: Endpoint,
    blobs: BlobsProtocol,
    accept: Arc<RwLock<HashMap<Share, oneshot::Sender<bool>>>>,
}

impl Fileshare {
    pub fn new(endpoint: Endpoint, blobs: BlobsProtocol) -> Self {
        Self {
            endpoint,
            blobs,
            accept: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn requests(&self) -> Vec<Share> {
        self.accept.read().await.keys().cloned().collect()
    }

    pub async fn accept(&self, share: &Share) -> color_eyre::Result<()> {
        if let Some(s) = self.accept.write().await.remove(share)
            && s.send(true).is_err()
        {
            bail!("Channel closed");
        }

        Ok(())
    }

    pub async fn reject(&self, share: &Share) -> color_eyre::Result<()> {
        if let Some(s) = self.accept.write().await.remove(share)
            && s.send(false).is_err()
        {
            bail!("Channel closed");
        }

        Ok(())
    }

    pub async fn send_share(
        &self,
        addr: impl Into<EndpointAddr>,
        share: Share,
    ) -> color_eyre::Result<bool> {
        let conn = self.endpoint.connect(addr, ALPN).await?;

        let (mut tx, mut rx) = conn.open_bi().await?;
        tx.write_all(HANDSHAKE).await?;

        let encoded = {
            let mut w = Cursor::new(Vec::new());
            ciborium::into_writer(&share, &mut w)?;
            w.into_inner()
        };
        tx.write_all(&(encoded.len() as u32).to_le_bytes()).await?;
        tx.write_all(&encoded).await?;

        let mut code = [0u8; MSG_LEN];
        rx.read_exact(&mut code).await?;

        let res = if code == MSG_OK {
            true
        } else if code == MSG_NO {
            false
        } else {
            bail!("Unknown code");
        };

        Ok(res)
    }

    pub async fn get_text_size(&self, ticket: &BlobTicket) -> color_eyre::Result<u64> {
        let conn = self
            .endpoint
            .connect(ticket.addr().clone(), iroh_blobs::ALPN)
            .await?;

        let (size, _stats) = get_verified_size(&conn, &ticket.hash()).await?;

        Ok(size)
    }

    pub async fn get_files_metadata(
        &self,
        ticket: &BlobTicket,
    ) -> color_eyre::Result<Vec<(String, u64)>> {
        let conn = self
            .endpoint
            .connect(ticket.addr().clone(), iroh_blobs::ALPN)
            .await?;

        let (_hashes, sizes) =
            get_hash_seq_and_sizes(&conn, &ticket.hash(), 16 * 1024 * 1024, None).await?;

        let collection = Self::get_collection_metadata(self, ticket).await?;

        let file_sizes = sizes
            .get(1..)
            .ok_or_else(|| eyre!("Missing collection metadata"))?;

        ensure!(
            collection.len() == file_sizes.len(),
            "Filename/size count mismatch"
        );

        Ok(collection
            .iter()
            .zip(file_sizes.iter())
            .map(|((name, _hash), size)| (name.clone(), *size))
            .collect())
    }

    pub async fn get_file_metadata(
        &self,
        ticket: &BlobTicket,
    ) -> color_eyre::Result<(String, u64)> {
        let mut files = self.get_files_metadata(ticket).await?;
        ensure!(files.len() == 1, "Expected exactly one file");

        files.pop().ok_or_else(|| eyre!("Missing file metadata"))
    }

    async fn insert(&self, share: Share) -> oneshot::Receiver<bool> {
        let (tx, rx) = oneshot::channel();
        self.accept.write().await.insert(share, tx);
        rx
    }

    async fn get_collection_metadata(&self, ticket: &BlobTicket) -> color_eyre::Result<Collection> {
        let conn = self
            .endpoint
            .connect(ticket.addr().clone(), iroh_blobs::ALPN)
            .await?;

        let request = GetRequest::builder()
            .root(ChunkRanges::all())
            .child(0, ChunkRanges::all())
            .build(ticket.hash());

        self.blobs
            .remote()
            .execute_get(conn, request)
            .complete()
            .await?;

        let collection = Collection::load(ticket.hash(), self.blobs.store()).await?;

        Ok(collection)
    }

    async fn task(&self, conn: Connection) -> color_eyre::Result<()> {
        let (mut tx, mut rx) = conn.accept_bi().await?;

        let mut handshake = [0u8; HANDSHAKE.len()];
        rx.read_exact(&mut handshake).await?;

        if handshake != HANDSHAKE {
            bail!("Invalid handshake");
        }

        let mut share_length_bytes = [0u8; 4];
        rx.read_exact(&mut share_length_bytes).await?;
        let share_length = u32::from_le_bytes(share_length_bytes) as usize;

        if share_length > 4 * 1024 {
            bail!("Share is too large");
        }

        let mut share_bytes = vec![0u8; share_length];
        rx.read_exact(&mut share_bytes).await?;
        let share: Share = ciborium::from_reader(Cursor::new(share_bytes))?;

        tracing::debug!("Decoded share {:?}", share);
        tracing::debug!("Awaiting accept/decline...");

        let rx = self.insert(share).await;

        if rx.await.unwrap_or(false) {
            tx.write_all(MSG_NO).await?;
        } else {
            tx.write_all(MSG_OK).await?;
        }

        Ok(())
    }
}

impl ProtocolHandler for Fileshare {
    async fn accept(&self, connection: Connection) -> Result<(), iroh::protocol::AcceptError> {
        if let Err(e) = self.task(connection).await {
            tracing::warn!("File share protocol error: {e:?}");
        }

        Ok(())
    }
}
