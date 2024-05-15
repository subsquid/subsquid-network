use clap::Parser;
use simple_logger::SimpleLogger;

use contract_client::{self, PeerId, RpcArgs};

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    rpc: RpcArgs,
    client_id: PeerId,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    SimpleLogger::new().with_level(log::LevelFilter::Info).env().init()?;
    let cli: Cli = Cli::parse();

    let client = contract_client::get_client(&cli.rpc).await?;
    let allocations = client.current_allocations(cli.client_id, None).await?;
    allocations.iter().for_each(|w| println!("{w:?}"));
    Ok(())
}
