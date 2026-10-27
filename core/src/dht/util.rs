use color_eyre::eyre::{Context, bail};
use iroh_mainline_endpoint_discovery::AddrIndex;
use n0_mainline::Dht;

pub async fn init(port: u16) -> color_eyre::Result<(Dht, AddrIndex)> {
    let dht = Dht::builder()
        .port(port)
        .build()
        .context("failed to start Mainline DHT")?;

    if !dht.bootstrapped().await? {
        bail!("Mainline DHT bootstrap failed");
    }

    let index = AddrIndex::builder(dht.clone())
        .n0_defaults()
        .build()
        .await
        .context("failed to discover address index")?;

    Ok((dht, index))
}
