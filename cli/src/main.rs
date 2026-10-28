mod cli;
mod recv;
mod send;

use airdroppe_core::{Node, NodeConfig, ShareType};
use clap::Parser;
use cli::{Cli, Mode};
use color_eyre::eyre::ContextCompat;
use directories::ProjectDirs;
use futures_util::StreamExt;
use std::path::PathBuf;
use tokio::{
    fs,
    io::{AsyncBufReadExt, BufReader, stdin},
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

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

                        // Get file
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

                        // Get files
                    }
                }
            }
        }
        Some(Mode::Send { text, path, code }) => {}
        None => {
            // TUI is a TODO
        }
    }

    Ok(())
}
