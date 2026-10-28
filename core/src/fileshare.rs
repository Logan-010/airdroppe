use color_eyre::eyre::{bail, ensure, eyre};
use futures_util::{Stream, stream};
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
use tokio::sync::{RwLock, broadcast, oneshot};

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
    finished: Arc<RwLock<HashMap<Share, oneshot::Sender<bool>>>>,
    requests: broadcast::Sender<Share>,
}

impl Fileshare {
    pub fn new(endpoint: Endpoint, blobs: BlobsProtocol) -> Self {
        Self {
            endpoint,
            blobs,
            accept: Arc::new(RwLock::new(HashMap::new())),
            finished: Arc::new(RwLock::new(HashMap::new())),
            requests: broadcast::channel(64).0,
        }
    }

    pub fn requests(
        &self,
    ) -> impl Stream<Item = Result<Share, broadcast::error::RecvError>> + Send + use<> {
        stream::unfold(self.requests.subscribe(), |mut rx| async move {
            match rx.recv().await {
                Err(broadcast::error::RecvError::Closed) => None,
                result => Some((result, rx)),
            }
        })
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
        self.send_share_inner(addr, share, false).await
    }

    /// Send a share and keep serving until the receiver confirms completion.
    pub async fn send_share_and_wait(
        &self,
        addr: impl Into<EndpointAddr>,
        share: Share,
    ) -> color_eyre::Result<bool> {
        self.send_share_inner(addr, share, true).await
    }

    pub async fn complete(&self, share: &Share, success: bool) {
        if let Some(tx) = self.finished.write().await.remove(share) {
            let _ = tx.send(success);
        }
    }

    async fn send_share_inner(
        &self,
        addr: impl Into<EndpointAddr>,
        share: Share,
        wait_for_completion: bool,
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
        tx.finish()?;

        let mut code = [0u8; MSG_LEN];
        rx.read_exact(&mut code).await?;

        let res = if code == MSG_OK {
            true
        } else if code == MSG_NO {
            false
        } else {
            bail!("Unknown code");
        };

        if res && wait_for_completion {
            rx.read_exact(&mut code).await?;
            ensure!(code == MSG_OK, "Receiver failed to save the share");
        }

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
        self.accept.write().await.insert(share.clone(), tx);
        if self.requests.send(share).is_err() {
            tracing::warn!("Failed to broadcast share");
        }
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

        let (finished_tx, finished_rx) = oneshot::channel();
        self.finished
            .write()
            .await
            .insert(share.clone(), finished_tx);
        let rx = self.insert(share.clone()).await;

        if rx.await.unwrap_or(false) {
            tx.write_all(MSG_OK).await?;
            let success = finished_rx.await.unwrap_or(false);
            tx.write_all(if success { MSG_OK } else { MSG_NO }).await?;
        } else {
            self.finished.write().await.remove(&share);
            tx.write_all(MSG_NO).await?;
        }

        // Keep the connection alive until the peer has received the final reply.
        tx.finish()?;
        tx.stopped().await?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{FutureExt, StreamExt};
    use iroh::{RelayMode, endpoint::presets};
    use iroh_blobs::store::mem::MemStore;

    async fn fileshare() -> Fileshare {
        let endpoint = Endpoint::builder(presets::N0)
            .relay_mode(RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        Fileshare::new(endpoint, BlobsProtocol::new(&MemStore::new(), None))
    }

    fn share(ticket: impl ToString) -> Share {
        Share {
            ticket: ticket.to_string(),
            share: ShareType::Text,
        }
    }

    #[tokio::test]
    async fn requests_subscribe_immediately_and_do_not_replay() {
        let fileshare = fileshare().await;
        let _old = fileshare.insert(share("old")).await;
        let requests = fileshare.requests();
        let other = fileshare.clone().requests();
        tokio::pin!(requests, other);

        let request = share("new");
        let response = fileshare.insert(request.clone()).await;
        assert_eq!(requests.next().await.unwrap().unwrap(), request);
        assert_eq!(other.next().await.unwrap().unwrap(), request);
        fileshare.accept(&request).await.unwrap();
        assert!(response.await.unwrap());

        let request = share("rejected");
        let response = fileshare.insert(request.clone()).await;
        assert_eq!(requests.next().await.unwrap().unwrap(), request);
        fileshare.reject(&request).await.unwrap();
        assert!(!response.await.unwrap());
        assert!(requests.next().now_or_never().is_none());

        fileshare.endpoint.close().await;
    }

    #[tokio::test]
    async fn requests_report_lag_and_continue() {
        let fileshare = fileshare().await;
        let requests = fileshare.requests();
        tokio::pin!(requests);
        for index in 0..65 {
            let _response = fileshare.insert(share(index)).await;
        }
        assert!(matches!(
            requests.next().await,
            Some(Err(broadcast::error::RecvError::Lagged(1)))
        ));
        assert_eq!(requests.next().await.unwrap().unwrap(), share(1));
        fileshare.endpoint.close().await;
    }

    #[tokio::test]
    async fn requests_end_when_all_fileshare_handles_are_dropped() {
        let fileshare = fileshare().await;
        let requests = fileshare.requests();
        tokio::pin!(requests);
        fileshare.endpoint.close().await;
        drop(fileshare);
        assert!(requests.next().await.is_none());
    }
}
