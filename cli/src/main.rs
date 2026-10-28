mod cli;
mod recv;
mod send;

use airdroppe_core::{Node, NodeConfig, Share, ShareType};
use clap::Parser;
use cli::{Cli, Mode};
use color_eyre::eyre::{ContextCompat, bail};
use directories::ProjectDirs;
use futures_util::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use iroh_blobs::ticket::BlobTicket;
use std::path::{Path, PathBuf};
use tokio::{
    fs,
    io::{AsyncBufReadExt, BufReader, stdin},
    sync::mpsc,
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

fn progress_bar(size: u64) -> ProgressBar {
    let pb = ProgressBar::new(size);
    pb.set_style(
                ProgressStyle::default_bar()
                    .template("{prefix} {spinner} [{elapsed_precise}] [{wide_bar}] {percent_precise} ({bytes_per_sec}, {eta})")
                    .unwrap()
                    .progress_chars("#>-"),
            );
    pb
}

async fn receive_with_progress(
    node: &Node,
    base: &Path,
    share: Share,
    size: u64,
) -> color_eyre::Result<()> {
    let bars = MultiProgress::new();
    let download = bars.add(progress_bar(size));
    download.set_prefix("Download");
    let export = bars.add(progress_bar(size));
    export.set_prefix("Export");

    let (download_tx, mut download_rx) = mpsc::channel(32);
    let (export_tx, mut export_rx) = mpsc::channel(32);

    let receive = async {
        match share.share {
            ShareType::File => {
                recv::recv_file(node, base, share, Some(download_tx), Some(export_tx)).await
            }
            ShareType::Files => {
                recv::recv_files(node, base, share, Some(download_tx), Some(export_tx)).await
            }
            ShareType::Text => unreachable!("Text transfers do not export files"),
        }
    };

    let (result, (), ()) = tokio::join!(
        receive,
        async {
            while let Some(bytes) = download_rx.recv().await {
                download.set_position(bytes.min(size));
            }
        },
        async {
            while let Some(bytes) = export_rx.recv().await {
                download.finish();
                export.set_position(bytes.min(size));
            }
        },
    );

    if result.is_ok() {
        download.finish();
        export.finish();
    } else {
        download.abandon();
        export.abandon();
    }

    result
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let cli = Cli::parse();

    if let Err(e) = tracing_subscriber::registry()
        .with(EnvFilter::new(&cli.logging))
        .with(tracing_subscriber::fmt::layer())
        .try_init()
    {
        eprintln!("Failed to initialize logger {e:?}");
    }

    let config_path = match cli.config {
        Some(c) => c,
        None => ProjectDirs::from("com", "Logan-010", "airdroppe")
            .context("Expected project directories")?
            .config_dir()
            .join("config.toml"),
    };

    if let Some(parent) = config_path.parent()
        && !fs::try_exists(parent).await?
    {
        fs::create_dir_all(parent).await?;
    }

    let config = if fs::try_exists(&config_path).await? {
        let data = fs::read(config_path).await?;
        toml::from_slice(&data)?
    } else {
        let store = ProjectDirs::from("com", "Logan-010", "airdroppe")
            .context("Expected project directories")?
            .data_dir()
            .join("store");

        let cfg = NodeConfig::new(store);

        fs::write(config_path, toml::to_string_pretty(&cfg)?).await?;

        cfg
    };

    let node = Node::new(config).await?;

    match cli.mode {
        Some(Mode::Recv { to, code_length }) => {
            let base = to.unwrap_or_else(|| PathBuf::from("."));
            let requests = node.fileshare().requests();
            tokio::pin!(requests);

            let code = airdroppe_core::code::generate(code_length);

            node.discovery().publish(&code, node.id(), None).await?;

            println!("Code \"{code}\"\nWaiting for requests...");

            'rl: while let Some(request) = requests.next().await {
                let request = match request {
                    Ok(request) => request,
                    Err(e) => {
                        tracing::warn!("Missed incoming share requests: {e}");
                        continue;
                    }
                };
                let Some(ticket) = request.ticket() else {
                    tracing::warn!("Skipping request with invalid ticket");
                    continue;
                };

                match request.share {
                    ShareType::Text => {
                        let size = match node.fileshare().get_text_size(&ticket).await {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!("Failed to get text size {e:?}");
                                continue;
                            }
                        };

                        println!("Accept text transfer ({size} bytes): ");

                        let mut lines = BufReader::new(stdin()).lines();
                        'sl: while let Some(line) = lines.next_line().await? {
                            match line.to_lowercase().as_str() {
                                "yes" | "y" | "o" | "ok" | "okay" => break 'sl,
                                "no" | "n" | "nope" => {
                                    if let Err(e) = node.fileshare().reject(&request).await {
                                        tracing::warn!("Reject share error: {e:?}");
                                    }
                                    continue 'rl;
                                }
                                _ => println!(
                                    "Please answer the question.\nAccept text transfer ({size} bytes): "
                                ),
                            }
                        }

                        if let Err(e) = node.fileshare().accept(&request).await {
                            tracing::warn!("Accept share error: {e:?}");
                            continue;
                        }

                        match recv::recv_text(&node, request).await {
                            Ok(t) => println!("{t}"),
                            Err(e) => tracing::warn!("Failed to get text {e:?}"),
                        }
                    }
                    ShareType::File => {
                        let (name, size) = match node.fileshare().get_file_metadata(&ticket).await {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!("Failed to get file metadata {e:?}");
                                continue;
                            }
                        };

                        println!("Accept file {name} ({size} bytes): ");

                        let mut lines = BufReader::new(stdin()).lines();
                        'sl: while let Some(line) = lines.next_line().await? {
                            match line.to_lowercase().as_str() {
                                "yes" | "y" | "o" | "ok" | "okay" => break 'sl,
                                "no" | "n" | "nope" => {
                                    if let Err(e) = node.fileshare().reject(&request).await {
                                        tracing::warn!("Reject share error: {e:?}");
                                    }
                                    continue 'rl;
                                }
                                _ => println!(
                                    "Please answer the question.\nAccept file {name} ({size} bytes): "
                                ),
                            }
                        }

                        if let Err(e) = node.fileshare().accept(&request).await {
                            tracing::warn!("Accept share error: {e:?}");
                            continue;
                        }

                        match receive_with_progress(&node, &base, request, size).await {
                            Ok(()) => println!("Received {name} in {}", base.display()),
                            Err(e) => tracing::warn!("Failed to receive file: {e:?}"),
                        }
                    }
                    ShareType::Files => {
                        let meta = match node.fileshare().get_files_metadata(&ticket).await {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!("Failed to get files metadata {e:?}");
                                continue;
                            }
                        };

                        println!("Accept files?");
                        meta.iter()
                            .for_each(|(name, size)| println!("{name} ({size} bytes)"));

                        let mut lines = BufReader::new(stdin()).lines();
                        'sl: while let Some(line) = lines.next_line().await? {
                            match line.to_lowercase().as_str() {
                                "yes" | "y" | "o" | "ok" | "okay" => break 'sl,
                                "no" | "n" | "nope" => {
                                    if let Err(e) = node.fileshare().reject(&request).await {
                                        tracing::warn!("Reject share error: {e:?}");
                                    }
                                    continue 'rl;
                                }
                                _ => {
                                    println!("Please answer the question.\nAccept files?");
                                    meta.iter()
                                        .for_each(|(name, size)| println!("{name} ({size} bytes)"));
                                }
                            }
                        }

                        if let Err(e) = node.fileshare().accept(&request).await {
                            tracing::warn!("Accept share error: {e:?}");
                            continue;
                        }

                        let size = meta.iter().map(|(_, size)| *size).sum();
                        match receive_with_progress(&node, &base, request, size).await {
                            Ok(()) => {
                                println!("Received {} files in {}", meta.len(), base.display())
                            }
                            Err(e) => tracing::warn!("Failed to receive files: {e:?}"),
                        }
                    }
                }
            }
        }
        Some(Mode::Send { text, path, code }) => {
            let Some(id) = node.discovery().resolve(&code, None).await? else {
                bail!("No providers for code");
            };

            let share = match (text, path) {
                (Some(t), None) => {
                    let tag = node.blobs().blobs().add_bytes(t).await?;
                    let ticket = BlobTicket::new(node.id().into(), tag.hash, tag.format);

                    Share::new(ticket, ShareType::Text)
                }
                (None, Some(p)) => {
                    todo!()
                }
                _ => unreachable!(),
            };

            if node.fileshare().send_share(id, share).await? {
                todo!()
            } else {
                tracing::warn!("Node rejected share");
            }
        }
        None => {
            // TUI is a TODO
        }
    }

    Ok(())
}
