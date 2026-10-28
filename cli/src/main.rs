mod cli;
mod progress;
mod recv;
mod send;

use airdroppe_core::{Node, NodeConfig, Share, ShareType};
use clap::Parser;
use cli::{Cli, Mode};
use color_eyre::eyre::{ContextCompat, bail};
use directories::ProjectDirs;
use futures_util::StreamExt;
use indicatif::MultiProgress;
use progress::progress_bar;
use std::path::{Path, PathBuf};
use tokio::{
    fs,
    io::{AsyncBufReadExt, BufReader, stdin},
    sync::mpsc,
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

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
        let completed_share = share.clone();
        let result = match share.share {
            ShareType::File => {
                recv::recv_file(node, base, share, Some(download_tx), Some(export_tx)).await
            }
            ShareType::Files => {
                recv::recv_files(node, base, share, Some(download_tx), Some(export_tx)).await
            }
            ShareType::Text => unreachable!("Text transfers do not export files"),
        };
        node.fileshare()
            .complete(&completed_share, result.is_ok())
            .await;
        result
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

    let result = tokio::select! {
        result = run(&node, cli.mode) => result,
        result = tokio::signal::ctrl_c() => result.map_err(Into::into),
    };

    node.shutdown().await?;
    result
}

async fn run(node: &Node, mode: Option<Mode>) -> color_eyre::Result<()> {
    match mode {
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

                        let result = recv::recv_text(&node, request.clone()).await;
                        node.fileshare().complete(&request, result.is_ok()).await;
                        match result {
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

            match (text, path) {
                (Some(t), None) => send::send_text(&node, t, id).await?,
                (None, Some(p)) => {
                    let (_, files) = send::path_files(&p).await?;
                    let size = files.iter().map(|(_, _, size)| *size).sum();
                    let bars = MultiProgress::new();
                    let import = bars.add(progress_bar(size));
                    import.set_prefix("Import");
                    let upload = bars.add(progress_bar(size));
                    upload.set_prefix("Upload");
                    let (import_tx, mut import_rx) = mpsc::channel(32);
                    let (upload_tx, mut upload_rx) = mpsc::channel(32);
                    let (result, (), ()) = tokio::join!(
                        send::send_path(node, &p, id, Some(import_tx), Some(upload_tx)),
                        async {
                            while let Some(bytes) = import_rx.recv().await {
                                import.set_position(bytes.min(size));
                            }
                            // Closing this channel marks a successful import, or an error.
                        },
                        async {
                            while let Some(bytes) = upload_rx.recv().await {
                                import.finish();
                                upload.set_position(bytes.min(size));
                            }
                        },
                    );
                    if result.is_ok() {
                        import.finish();
                        upload.finish();
                        println!("Sent {}", p.display());
                    } else {
                        import.abandon();
                        upload.abandon();
                    }
                    result?;
                }
                _ => unreachable!(),
            }
        }
        None => {
            // TUI is a TODO
        }
    }

    Ok(())
}
