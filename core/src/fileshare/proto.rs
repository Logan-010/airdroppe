use super::{Fileshare, Share};
use color_eyre::eyre::bail;
use iroh::{Endpoint, EndpointAddr, endpoint::Connection, protocol::ProtocolHandler};
use std::io::Cursor;

pub const ALPN: &[u8] = b"/airdroppe/fileshare/1.0.0";
pub const HANDSHAKE: &[u8] = b"i cant be all about like, spelling and sh*t";

const MSG_LEN: usize = 2;
const MSG_NO: &[u8] = b"no";
const MSG_OK: &[u8] = b"ok";

async fn inner(fileshare: &Fileshare, conn: Connection) -> color_eyre::Result<()> {
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

    let rx = fileshare.insert(share).await;

    if rx.await.unwrap_or(false) {
        tx.write_all(MSG_NO).await?;
    } else {
        tx.write_all(MSG_OK).await?;
    }

    Ok(())
}

pub(super) async fn drop(
    endpoint: &Endpoint,
    addr: impl Into<EndpointAddr>,
    share: Share,
) -> color_eyre::Result<bool> {
    let conn = endpoint.connect(addr, ALPN).await?;

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

impl ProtocolHandler for Fileshare {
    async fn accept(&self, connection: Connection) -> Result<(), iroh::protocol::AcceptError> {
        if let Err(e) = inner(self, connection).await {
            tracing::warn!("File share protocol error: {e:?}");
        }

        Ok(())
    }
}
