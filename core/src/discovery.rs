use color_eyre::eyre::{Context, ContextCompat, ensure};
use iroh::{
    EndpointId, SecretKey,
    address_lookup::{N0_DNS_PKARR_RELAY_PROD, PkarrError, PkarrRelayClient},
    dns::DnsResolver,
    endpoint::Endpoint,
};
use iroh_dns::pkarr::SignedPacket;
use std::{error::Error, time::Duration};
use url::Url;

const RECORD_NAME: &str = "_codedisc";
const RECORD_VERSION: &str = "v1";
const KEY_CONTEXT: &str = "Airedroppe DHT Discovery v1";
const TTL: u32 = 300;
const TIMEOUT: Duration = Duration::from_secs(15);

/// Publishes and resolves connect codes using a pkarr relay.
pub struct PeerDiscovery {
    client: PkarrRelayClient,
}

impl PeerDiscovery {
    /// Create a discovery client using an existing Iroh endpoint.
    pub fn new(endpoint: &Endpoint, pkarr_relay: Option<&str>) -> color_eyre::Result<Self> {
        let dns: DnsResolver = endpoint
            .dns_resolver()
            .context("endpoint has no DNS resolver")?
            .clone();

        let client = PkarrRelayClient::new(
            Url::parse(pkarr_relay.unwrap_or(N0_DNS_PKARR_RELAY_PROD))?,
            endpoint.tls_config().clone(),
            dns,
        );
        Ok(Self { client })
    }

    /// Derive a deterministic discovery key from the connect code.
    fn discovery_key(code: &str) -> SecretKey {
        SecretKey::from(blake3::derive_key(KEY_CONTEXT, code.as_bytes()))
    }

    /// Return the public lookup ID for a connect code.
    pub fn discovery_id(code: &str) -> EndpointId {
        Self::discovery_key(code).public()
    }

    /// Device A: publish its real endpoint ID.
    pub async fn publish(
        &self,
        code: &str,
        peer: EndpointId,
        timeout: Option<Duration>,
    ) -> color_eyre::Result<()> {
        let key = Self::discovery_key(code);

        let packet = SignedPacket::from_txt_strings(
            &key,
            RECORD_NAME,
            [RECORD_VERSION.to_string(), format!("peer,{peer}")],
            TTL,
        )?;

        tokio::time::timeout(timeout.unwrap_or(TIMEOUT), self.client.publish(&packet))
            .await
            .context("publish timed out")??;

        Ok(())
    }

    /// Device B: resolve the code into Device A's endpoint ID.
    /// Returns None for missing or deleted records.
    pub async fn resolve(
        &self,
        code: &str,
        timeout: Option<Duration>,
    ) -> color_eyre::Result<Option<EndpointId>> {
        let id = Self::discovery_id(code);

        let packet = match tokio::time::timeout(timeout.unwrap_or(TIMEOUT), self.client.resolve(id))
            .await
            .context("resolve timed out")?
        {
            Ok(packet) => packet,
            Err(e) if Self::is_not_found(&e) => {
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        ensure!(packet.public_key() == id, "discovery key mismatch");

        let records = packet.txt_records(RECORD_NAME);

        ensure!(
            records.first().is_some_and(|r| r == RECORD_VERSION),
            "unsupported discovery record"
        );

        if records.iter().any(|r| r == "deleted") {
            return Ok(None);
        }

        let peer = records
            .iter()
            .find_map(|r| r.strip_prefix("peer,"))
            .context("missing peer ID")?
            .parse::<EndpointId>()?;

        Ok(Some(peer))
    }

    /// Device A: replace the record with a deletion marker.
    pub async fn delete(&self, code: &str, timeout: Option<Duration>) -> color_eyre::Result<()> {
        let key = Self::discovery_key(code);

        let packet = SignedPacket::from_txt_strings(
            &key,
            RECORD_NAME,
            [RECORD_VERSION.to_string(), "deleted".to_string()],
            TTL,
        )?;

        tokio::time::timeout(timeout.unwrap_or(TIMEOUT), self.client.publish(&packet))
            .await
            .context("delete timed out")??;

        Ok(())
    }

    fn is_not_found(error: &(dyn Error + 'static)) -> bool {
        while let Some(error) = error.source() {
            if let Some(PkarrError::HttpRequest { status, .. }) = error.downcast_ref::<PkarrError>()
            {
                return status.as_u16() == 404;
            }
        }

        false
    }
}
