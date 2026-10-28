use crate::{
    discovery::PeerDiscovery,
    fileshare::{self, Fileshare},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use color_eyre::eyre::ContextCompat;
use futures_util::{Stream, StreamExt};
use iroh::{
    Endpoint, PublicKey, RelayMap, RelayMode, RelayUrl, SecretKey, endpoint::presets,
    protocol::Router,
};
use iroh_blobs::{BlobsProtocol, store::fs::FsStore};
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc};
use tokio::{pin, select, sync::RwLock, task};

#[derive(Serialize, Deserialize)]
pub struct NodeConfig {
    identity: String,
    store: PathBuf,
    address: Option<SocketAddr>,
    pkarr_relay_address: Option<String>,
    relay_map: Option<String>,
}

impl NodeConfig {
    pub fn new(store: PathBuf) -> Self {
        let identity = STANDARD.encode(SecretKey::generate().to_bytes());

        Self {
            identity,
            store,
            address: None,
            pkarr_relay_address: None,
            relay_map: None,
        }
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

        if let Some(r) = config.relay_map {
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

        let blobs = BlobsProtocol::new(&store, None);

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
        })
    }

    pub async fn shutdown(&self) -> color_eyre::Result<()> {
        self.router.shutdown().await?;
        self.blobs.shutdown().await?;
        self.store.shutdown().await?;
        self.endpoint.close().await;
        Ok(())
    }

    pub async fn id(&self) -> PublicKey {
        self.endpoint.secret_key().public()
    }

    pub async fn clean(&self) -> color_eyre::Result<()> {
        self.store.tags().delete_all().await?;

        Ok(())
    }
}
