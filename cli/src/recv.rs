use airdroppe_core::{Node, Share, ShareType};
use color_eyre::eyre::{ContextCompat, bail};
use futures_util::StreamExt;
use iroh_blobs::{
    api::{blobs::ExportProgressItem, remote::GetProgressItem},
    format::collection::Collection,
};
use std::path::{Component, Path};
use tokio::{fs, sync::mpsc};

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

pub async fn recv_file(
    node: &Node,
    base: &Path,
    share: Share,
    download_progress_tx: Option<mpsc::Sender<u64>>,
    export_progress_tx: Option<mpsc::Sender<u64>>,
) -> color_eyre::Result<()> {
    assert_eq!(share.share, ShareType::File);

    let ticket = share.ticket().context("Invalid ticket")?;

    let conn = node.blob_conn(&ticket).await?;

    let mut progress = node
        .blobs()
        .remote()
        .fetch(conn, ticket.hash_and_format())
        .stream();

    while let Some(p) = progress.next().await {
        match p {
            GetProgressItem::Progress(n) => {
                if let Some(tx) = download_progress_tx.as_ref()
                    && tx.send(n).await.is_err() {
                        tracing::warn!("Download progress channel closed");
                    }
            }
            GetProgressItem::Done(_) => break,
            GetProgressItem::Error(e) => bail!(e),
        }
    }

    let collection = Collection::load(ticket.hash(), node.blobs().store()).await?;

    if collection.len() != 1 {
        bail!("Expected exactly one file");
    }

    let (name, hash) = collection.iter().next().context("Missing file")?;

    let mut components = Path::new(name).components();
    if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
        bail!("Invalid filename: {name}");
    }

    if !fs::try_exists(&base).await? {
        fs::create_dir_all(base).await?;
    }

    let destination = fs::canonicalize(base).await?.join(name);

    let mut progress = node
        .blobs()
        .blobs()
        .export(*hash, &destination)
        .stream()
        .await;

    while let Some(p) = progress.next().await {
        match p {
            ExportProgressItem::CopyProgress(n) => {
                if let Some(tx) = export_progress_tx.as_ref()
                    && tx.send(n).await.is_err() {
                        tracing::warn!("Export progress channel closed");
                    }
            }
            ExportProgressItem::Done => break,
            ExportProgressItem::Error(e) => bail!(e),
            _ => {}
        }
    }

    Ok(())
}

pub async fn recv_files(
    node: &Node,
    base: &Path,
    share: Share,
    download_progress_tx: Option<mpsc::Sender<u64>>,
    export_progress_tx: Option<mpsc::Sender<u64>>,
) -> color_eyre::Result<()> {
    assert_eq!(share.share, ShareType::Files);

    let ticket = share.ticket().context("Invalid ticket")?;

    let conn = node.blob_conn(&ticket).await?;

    let mut progress = node
        .blobs()
        .remote()
        .fetch(conn, ticket.hash_and_format())
        .stream();

    while let Some(p) = progress.next().await {
        match p {
            GetProgressItem::Progress(n) => {
                if let Some(tx) = download_progress_tx.as_ref()
                    && tx.send(n).await.is_err() {
                        tracing::warn!("Download progress channel closed");
                    }
            }
            GetProgressItem::Done(_) => break,
            GetProgressItem::Error(e) => bail!(e),
        }
    }

    let collection = Collection::load(ticket.hash(), node.blobs().store()).await?;

    let mut exported_bytes = 0;

    for (name, hash) in collection.into_iter() {
        let mut components = Path::new(&name).components();
        if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
            bail!("Invalid filename: {name}");
        }

        if !fs::try_exists(&base).await? {
            fs::create_dir_all(base).await?;
        }

        let destination = fs::canonicalize(base).await?.join(name);

        let mut progress = node
            .blobs()
            .blobs()
            .export(hash, &destination)
            .stream()
            .await;

        let mut file_size = 0;

        while let Some(p) = progress.next().await {
            match p {
                ExportProgressItem::Size(size) => file_size = size,
                ExportProgressItem::CopyProgress(n) => {
                    if let Some(tx) = export_progress_tx.as_ref()
                        && tx.send(exported_bytes + n).await.is_err() {
                            tracing::warn!("Export progress channel closed");
                        }
                }
                ExportProgressItem::Done => {
                    exported_bytes += file_size;
                    if let Some(tx) = export_progress_tx.as_ref()
                        && tx.send(exported_bytes).await.is_err() {
                            tracing::warn!("Export progress channel closed");
                        }
                    break;
                }
                ExportProgressItem::Error(e) => bail!(e),
            }
        }
    }

    Ok(())
}
