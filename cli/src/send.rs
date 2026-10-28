use airdroppe_core::{Node, Share, ShareType};
use color_eyre::eyre::{ContextCompat, bail, ensure};
use futures_util::StreamExt;
use iroh::PublicKey;
use iroh_blobs::{api::blobs::AddProgressItem, format::collection::Collection, ticket::BlobTicket};
use std::path::{Path, PathBuf};
use tokio::{
    fs,
    sync::{broadcast, mpsc},
};

pub async fn send_text(node: &Node, text: String, id: PublicKey) -> color_eyre::Result<()> {
    let tag = node.blobs().blobs().add_bytes(text).await?;
    let ticket = BlobTicket::new(node.id().into(), tag.hash, tag.format);
    let share = Share::new(ticket, ShareType::Text);

    if !node.fileshare().send_share_and_wait(id, share).await? {
        bail!("Node rejected share");
    }

    Ok(())
}

pub async fn send_path(
    node: &Node,
    path: &Path,
    id: PublicKey,
    import_tx: Option<mpsc::Sender<u64>>,
    upload_tx: Option<mpsc::Sender<u64>>,
) -> color_eyre::Result<()> {
    let (is_directory, files) = path_files(path).await?;
    let mut collection = Collection::default();
    let mut tags = Vec::new();
    let mut imported_bytes = 0;

    for (name, path, _) in files {
        let mut progress = node.blobs().blobs().add_path(path).stream().await;
        let mut file_size = 0;
        let mut tag = None;
        while let Some(item) = progress.next().await {
            match item {
                AddProgressItem::Size(size) => file_size = size,
                AddProgressItem::OutboardProgress(bytes) => {
                    report_progress(&import_tx, imported_bytes + bytes).await;
                }
                AddProgressItem::Done(value) => {
                    tag = Some(value);
                    break;
                }
                AddProgressItem::Error(error) => bail!(error),
                _ => {}
            }
        }
        let tag = tag.context("Import ended without completing")?;
        collection.push(name, tag.hash());
        tags.push(tag);
        imported_bytes += file_size;
        report_progress(&import_tx, imported_bytes).await;
    }
    drop(import_tx);

    // Keep all temporary tags alive until the receiver finishes saving the files.
    let collection_tag = collection.store(node.blobs().store()).await?;
    let hash = collection_tag.hash();
    let ticket = BlobTicket::new(node.id().into(), hash, collection_tag.format());
    let share = Share::new(
        ticket,
        if is_directory {
            ShareType::Files
        } else {
            ShareType::File
        },
    );
    let mut events = node.upload_progress();
    let send = node.fileshare().send_share_and_wait(id, share);
    tokio::pin!(send);

    let mut uploaded_bytes = 0;
    loop {
        tokio::select! {
            result = &mut send => {
                ensure!(result?, "Node rejected share");
                report_progress(&upload_tx, imported_bytes).await;
                break;
            }
            event = events.recv() => match event {
                Ok((peer, root, bytes)) if peer == id && root == hash => {
                    uploaded_bytes = uploaded_bytes.max(bytes).min(imported_bytes);
                    report_progress(&upload_tx, uploaded_bytes).await;
                }
                Err(broadcast::error::RecvError::Closed) => bail!("Upload progress stream closed"),
                _ => {}
            }
        }
    }
    drop(collection_tag);
    drop(tags);

    Ok(())
}

async fn report_progress(tx: &Option<mpsc::Sender<u64>>, bytes: u64) {
    if let Some(tx) = tx {
        let _ = tx.send(bytes).await;
    }
}

/// Collect files without following symlinks, preserving directory-relative names.
pub async fn path_files(path: &Path) -> color_eyre::Result<(bool, Vec<(String, PathBuf, u64)>)> {
    let metadata = fs::symlink_metadata(path).await?;
    ensure!(
        metadata.is_file() || metadata.is_dir(),
        "Expected a regular file or directory"
    );
    let is_directory = metadata.is_dir();
    let base = if is_directory {
        path
    } else {
        path.parent().context("Missing parent directory")?
    };
    let mut pending = vec![path.to_path_buf()];
    let mut files = Vec::new();
    while let Some(path) = pending.pop() {
        let metadata = fs::symlink_metadata(&path).await?;
        if metadata.is_dir() {
            let mut entries = fs::read_dir(path).await?;
            while let Some(entry) = entries.next_entry().await? {
                pending.push(entry.path());
            }
        } else {
            ensure!(metadata.is_file(), "Not a regular file: {}", path.display());
            let name = path
                .strip_prefix(base)?
                .to_str()
                .context("Filename is not UTF-8")?
                .to_owned();
            files.push((name, path, metadata.len()));
        }
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    ensure!(!files.is_empty(), "Directory contains no files");
    Ok((is_directory, files))
}
