use crate::{
    discovery::PeerDiscovery,
    fileshare::{self, Fileshare},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use color_eyre::eyre::ContextCompat;
use futures_util::{Stream, StreamExt};
use iroh::{
    Endpoint, PublicKey, RelayMap, RelayMode, RelayUrl, SecretKey,
    endpoint::{Connection, presets},
    protocol::Router,
};
use iroh_blobs::{
    BlobsProtocol, Hash,
    provider::events::{
        ConnectMode, EventMask, EventSender, ProviderMessage, RequestMode, RequestUpdate,
    },
    store::fs::FsStore,
    ticket::BlobTicket,
};
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};
use tokio::{
    pin, select,
    sync::{RwLock, broadcast},
    task,
};

#[derive(Serialize, Deserialize, Clone)]
pub struct NodeConfig {
    identity: String,
    store: PathBuf,
    address: Option<SocketAddr>,
    pkarr_relay_address: Option<String>,
    relay_address: Option<String>,
}

impl NodeConfig {
    pub fn new(store: PathBuf) -> Self {
        let identity = STANDARD.encode(SecretKey::generate().to_bytes());

        Self {
            identity,
            store,
            address: None,
            pkarr_relay_address: None,
            relay_address: None,
        }
    }

    pub fn with_address(self, address: Option<SocketAddr>) -> Self {
        Self { address, ..self }
    }

    pub fn with_pkarr_relay_address(self, pkarr_relay_address: Option<String>) -> Self {
        Self {
            pkarr_relay_address,
            ..self
        }
    }

    pub fn wtih_relay_map(self, relay_address: Option<String>) -> Self {
        Self {
            relay_address,
            ..self
        }
    }

    pub fn with_new_identity(self) -> Self {
        let identity = STANDARD.encode(SecretKey::generate().to_bytes());

        Self { identity, ..self }
    }

    fn identity(&self) -> Option<SecretKey> {
        let data = STANDARD.decode(&self.identity).ok()?;
        if data.len() != 32 {
            None
        } else {
            let mut raw = [0u8; 32];
            raw.clone_from_slice(&data);
            Some(SecretKey::from_bytes(&raw))
        }
    }
}

pub struct Node {
    endpoint: Endpoint,
    discovery: PeerDiscovery,
    store: FsStore,
    blobs: BlobsProtocol,
    fileshare: Fileshare,
    router: Router,
    nearby: Arc<RwLock<HashSet<PublicKey>>>,
    upload_progress: broadcast::Sender<(PublicKey, Hash, u64)>,
}

async fn mdns_task(
    stream: impl Stream<Item = DiscoveryEvent>,
    nearby: Arc<RwLock<HashSet<PublicKey>>>,
) {
    pin!(stream);

    while let Some(e) = stream.next().await {
        match e {
            DiscoveryEvent::Discovered { endpoint_info, .. } => {
                tracing::info!("Discovered {endpoint_info:?}");
                nearby.write().await.insert(endpoint_info.endpoint_id);
            }
            DiscoveryEvent::Expired { endpoint_id } => {
                tracing::warn!("Endpoint {} expired", endpoint_id);
                nearby.write().await.remove(&endpoint_id);
            }
            e => tracing::error!("Unhandled event {e:?}"),
        }
    }
}

impl Node {
    pub async fn new(config: NodeConfig) -> color_eyre::Result<Self> {
        let key = config.identity().context("Invalid identity in config")?;

        let mut endpoint_builder = Endpoint::builder(presets::N0).secret_key(key.clone());

        let mdns = MdnsAddressLookup::builder().build(key.public())?;

        endpoint_builder = endpoint_builder.address_lookup(mdns.clone());

        if let Some(a) = config.address {
            endpoint_builder = endpoint_builder.bind_addr(a)?;
        }

        if let Some(r) = config.relay_address {
            endpoint_builder =
                endpoint_builder.relay_mode(RelayMode::Custom(RelayMap::from_iter(vec![
                    RelayUrl::from_str(r.as_str())?,
                ])));
        }

        let endpoint = endpoint_builder.bind().await?;

        let nearby = Arc::new(RwLock::new(HashSet::new()));

        let s = mdns.subscribe().await;
        let n = nearby.clone();
        let e = endpoint.clone();
        task::spawn(async move {
            select! {
                _ = e.closed() => (),
                _ = mdns_task(s, n) => ()
            }
        });

        let discovery = PeerDiscovery::new(&endpoint, config.pkarr_relay_address.as_deref())?;

        let store = FsStore::load(&config.store).await?;

        let (upload_progress, _) = broadcast::channel(256);
        let (events, mut events_rx) = EventSender::channel(
            64,
            EventMask {
                connected: ConnectMode::Notify,
                get: RequestMode::NotifyLog,
                ..EventMask::DEFAULT
            },
        );
        let progress = upload_progress.clone();
        task::spawn(async move {
            let mut peers = HashMap::new();
            while let Some(event) = events_rx.recv().await {
                match event {
                    ProviderMessage::ClientConnectedNotify(msg) => {
                        if let Some(peer) = msg.endpoint_id {
                            peers.insert(msg.connection_id, peer);
                        }
                    }
                    ProviderMessage::ConnectionClosed(msg) => {
                        peers.remove(&msg.connection_id);
                    }
                    ProviderMessage::GetRequestReceivedNotify(mut msg) => {
                        let peer = peers.get(&msg.connection_id).copied();
                        let hash = msg.request.hash;
                        let progress = progress.clone();
                        task::spawn(async move {
                            let mut completed = 0;
                            let mut current_size = 0;
                            while let Ok(Some(update)) = msg.rx.recv().await {
                                let bytes = match update {
                                    RequestUpdate::Started(started) => {
                                        completed += current_size;
                                        current_size =
                                            if started.index >= 2 { started.size } else { 0 };
                                        completed
                                    }
                                    RequestUpdate::Progress(update) => {
                                        completed + update.end_offset.min(current_size)
                                    }
                                    RequestUpdate::Completed(_) => completed + current_size,
                                    RequestUpdate::Aborted(_) => break,
                                };
                                if let Some(peer) = peer {
                                    let _ = progress.send((peer, hash, bytes));
                                }
                            }
                        });
                    }
                    _ => {}
                }
            }
        });
        let blobs = BlobsProtocol::new(&store, Some(events));

        let fileshare = Fileshare::new(endpoint.clone(), blobs.clone());

        let router = Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs.clone())
            .accept(fileshare::ALPN, fileshare.clone())
            .spawn();

        Ok(Self {
            endpoint,
            discovery,
            store,
            blobs,
            fileshare,
            router,
            nearby,
            upload_progress,
        })
    }

    pub async fn blob_conn(&self, ticket: &BlobTicket) -> color_eyre::Result<Connection> {
        Ok(self
            .endpoint
            .connect(ticket.addr().clone(), iroh_blobs::ALPN)
            .await?)
    }

    pub async fn nearby(&self) -> HashSet<PublicKey> {
        self.nearby.read().await.clone()
    }

    pub fn discovery(&self) -> &PeerDiscovery {
        &self.discovery
    }

    pub fn fileshare(&self) -> &Fileshare {
        &self.fileshare
    }

    pub fn blobs(&self) -> &BlobsProtocol {
        &self.blobs
    }

    /// Subscribe to payload-byte upload progress, identified by peer and collection hash.
    pub fn upload_progress(&self) -> broadcast::Receiver<(PublicKey, Hash, u64)> {
        self.upload_progress.subscribe()
    }

    pub async fn shutdown(&self) -> color_eyre::Result<()> {
        // The router shuts down BlobsProtocol, which shuts down its backing store.
        self.router.shutdown().await?;
        self.endpoint.close().await;
        Ok(())
    }

    pub fn id(&self) -> PublicKey {
        self.endpoint.secret_key().public()
    }

    pub async fn clean(&self) -> color_eyre::Result<()> {
        self.store.tags().delete_all().await?;

        Ok(())
    }
}
