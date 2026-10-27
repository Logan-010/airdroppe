use blake3::Hash;
use color_eyre::eyre::Context;
use iroh::{Endpoint, EndpointId};
use iroh_mainline_endpoint_discovery::{AddrIndex, Publisher, Resolver, infohash_from_blake3};
use n0_mainline::{Dht, Id};
use std::time::Duration;
use tokio::time::timeout;

pub struct PeerDiscovery {
    // Keep publisher alive to renew announcements.
    _publisher: Publisher,
    resolver: Resolver,
    infohash: Id,
    self_id: EndpointId,
    lookup_timeout: Duration,
}

const DISCOVERY_DOMAIN: &str = "Airdroppe DHT discovery v1.0.0";

fn discovery_infohash(psk: &[u8]) -> Id {
    let key = blake3::derive_key(DISCOVERY_DOMAIN, psk);

    let hash = Hash::from_bytes(key);
    Id::from(infohash_from_blake3(&hash))
}

impl PeerDiscovery {
    pub async fn new(
        endpoint: &Endpoint,
        dht: Dht,
        index: AddrIndex,
        psk: &[u8],
        timeout: Option<Duration>,
    ) -> color_eyre::Result<Self> {
        let infohash = discovery_infohash(psk);

        let publisher = Publisher::new(endpoint.secret_key().clone(), dht.clone(), index.clone());

        publisher.add_infohash(infohash);

        let resolver = Resolver::new(dht, index);

        Ok(Self {
            _publisher: publisher,
            resolver,
            infohash,
            self_id: endpoint.id(),
            lookup_timeout: timeout.unwrap_or(Duration::from_secs(15)),
        })
    }

    pub async fn find_peers(&self) -> color_eyre::Result<Vec<EndpointId>> {
        let peers = timeout(self.lookup_timeout, self.resolver.resolve(self.infohash))
            .await
            .context("peer discovery timed out")??;

        Ok(peers.into_iter().filter(|id| *id != self.self_id).collect())
    }
}
