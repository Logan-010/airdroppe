use airdroppe_core::{Node, Share, ShareType};
use color_eyre::eyre::ContextCompat;
use std::path::Path;
use tokio::sync::mpsc;

pub async fn recv_text(node: &Node, share: Share) -> color_eyre::Result<String> {
    assert_eq!(share.share, ShareType::Text);

    let ticket = share.ticket().context("Invalid ticket")?;

    let conn = node.blob_conn(&ticket).await?;

    node.blobs()
        .remote()
        .fetch(conn, ticket.hash_and_format())
        .await?;

    let bytes = node.blobs().blobs().get_bytes(ticket.hash()).await?;
    let text = String::from_utf8(bytes.to_vec())?;

    Ok(text)
}

pub fn recv_file(
    node: &Node,
    base: &Path,
    share: Share,
    progress_tx: Option<mpsc::Sender<u64>>,
) -> color_eyre::Result<()> {
    assert_eq!(share.share, ShareType::File);
    Ok(())
}

pub fn recv_files(
    node: &Node,
    base: &Path,
    share: Share,
    progress_tx: Option<mpsc::Sender<u64>>,
) -> color_eyre::Result<()> {
    assert_eq!(share.share, ShareType::Files);
    Ok(())
}
