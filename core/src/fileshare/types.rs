use super::drop;
use color_eyre::eyre::{bail, ensure, eyre};
use iroh::{Endpoint, EndpointAddr};
use iroh_blobs::{
    BlobsProtocol,
    format::collection::Collection,
    get::request::{get_hash_seq_and_sizes, get_verified_size},
    protocol::{ChunkRanges, GetRequest},
    ticket::BlobTicket,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{RwLock, oneshot};

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
    #[allow(clippy::type_complexity)]
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

    pub async fn drop(
        &self,
        addr: impl Into<EndpointAddr>,
        share: Share,
    ) -> color_eyre::Result<bool> {
        drop(&self.endpoint, addr, share).await
    }

    pub(super) async fn insert(&self, share: Share) -> oneshot::Receiver<bool> {
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

    pub async fn get_text_metadata(&self, ticket: &BlobTicket) -> color_eyre::Result<u64> {
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
}
