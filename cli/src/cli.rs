use clap::{ArgGroup, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = env!("CARGO_PKG_NAME"))]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"))]
#[command(author = env!("CARGO_PKG_AUTHORS"))]
pub struct Cli {
    /// Set log level
    #[arg(long, short = 'L', env = "RUST_LOG", default_value_t = String::from(concat!(env!("CARGO_PKG_NAME"), "=warn,error")))]
    pub logging: String,

    /// Config path
    #[arg(long, short = 'c')]
    pub config: Option<PathBuf>,

    /// Use directly from the CLI instead of launching a TUI
    #[clap(subcommand)]
    pub mode: Option<Mode>,
}

#[derive(Subcommand)]
pub enum Mode {
    /// Receive data
    Recv {
        /// Save file(s) to a base path
        #[arg(long, short = 't')]
        to: Option<PathBuf>,

        /// Code length
        #[arg(long, short = 'c', default_value_t = 3)]
        code_length: usize,
    },
    #[command(group(
        ArgGroup::new("input")
            .args(["text", "path"])
            .required(true)
            .multiple(false)
    ))]
    /// Send data
    Send {
        /// Send raw text
        #[arg(long, short = 't')]
        text: Option<String>,

        /// Send file(s)
        #[arg(long, short = 'p')]
        path: Option<PathBuf>,

        /// Receiver code
        #[arg(long, short = 'c', env = "AIRDROPPE_CODE")]
        code: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_accepts_text_or_path() {
        for input in [["-t", "hello"], ["-p", "example.txt"]] {
            assert!(
                Cli::try_parse_from([
                    "airdroppe-cli",
                    "send",
                    "-c",
                    "paddle-choose-sample",
                    input[0],
                    input[1],
                ])
                .is_ok()
            );
        }
    }

    #[test]
    fn send_requires_exactly_one_input() {
        assert!(
            Cli::try_parse_from(["airdroppe-cli", "send", "-c", "paddle-choose-sample",]).is_err()
        );
        assert!(
            Cli::try_parse_from([
                "airdroppe-cli",
                "send",
                "-c",
                "paddle-choose-sample",
                "-t",
                "hello",
                "-p",
                "example.txt",
            ])
            .is_err()
        );
    }
}
