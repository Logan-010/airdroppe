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
        code: String,
    },
}
