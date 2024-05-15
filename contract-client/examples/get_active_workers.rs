use clap::Parser;
use simple_logger::SimpleLogger;

use contract_client::{self, RpcArgs};

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    rpc: RpcArgs,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    SimpleLogger::new().with_level(log::LevelFilter::Info).env().init()?;
    let cli: Cli = Cli::parse();

    let client = contract_client::get_client(&cli.rpc).await?;
    let workers = client.active_workers().await?;
    workers.iter().for_each(|w| println!("{w:?}"));
    Ok(())
}
