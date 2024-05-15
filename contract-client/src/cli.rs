use clap::{Args, ValueEnum};

use crate::Address;

#[derive(Args)]
pub struct RpcArgs {
    #[arg(
        long,
        env,
        help = "Blockchain RPC URL",
        default_value = "http://127.0.0.1:8545/"
    )]
    pub rpc_url: String,
    #[arg(
        long,
        env,
        help = "Layer 1 blockchain RPC URL. If not provided, rpc_url is assumed to be L1"
    )]
    pub l1_rpc_url: Option<String>,
    #[command(flatten)]
    contract_addrs: ContractAddrs,
    #[arg(long, env, help = "Network to connect to (mainnet or testnet)")]
    pub network: Network,
}

impl RpcArgs {
    pub fn gateway_registry_addr(&self) -> Address {
        self.contract_addrs
            .gateway_registry_contract_addr
            .unwrap_or_else(|| self.network.gateway_registry_default_addr())
    }

    pub fn worker_registration_addr(&self) -> Address {
        self.contract_addrs
            .worker_registration_contract_addr
            .unwrap_or_else(|| self.network.worker_registration_default_addr())
    }

    pub fn network_controller_addr(&self) -> Address {
        self.contract_addrs
            .network_controller_contract_addr
            .unwrap_or_else(|| self.network.network_controller_default_addr())
    }

    pub fn allocations_viewer_addr(&self) -> Address {
        self.contract_addrs
            .allocations_viewer_contract_addr
            .unwrap_or_else(|| self.network.allocations_viewer_default_addr())
    }

    pub fn multicall_addr(&self) -> Address {
        self.contract_addrs
            .multicall_contract_addr
            .unwrap_or_else(|| self.network.multicall_default_addr())
    }
}

#[derive(Args)]
pub struct ContractAddrs {
    #[arg(long, env)]
    pub gateway_registry_contract_addr: Option<Address>,
    #[arg(long, env)]
    pub worker_registration_contract_addr: Option<Address>,
    #[arg(long, env)]
    pub network_controller_contract_addr: Option<Address>,
    #[arg(long, env)]
    pub allocations_viewer_contract_addr: Option<Address>,
    #[arg(long, env)]
    pub multicall_contract_addr: Option<Address>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "kebab_case")]
pub enum Network {
    Tethys,
    Mainnet,
}

impl Network {
    pub fn gateway_registry_default_addr(&self) -> Address {
        match self {
            Network::Tethys => "0xAB46F688AbA4FcD1920F21E9BD16B229316D8b0a".parse().unwrap(),
            Network::Mainnet => "0x8A90A1cE5fa8Cf71De9e6f76B7d3c0B72feB8c4b".parse().unwrap(),
        }
    }

    pub fn worker_registration_default_addr(&self) -> Address {
        match self {
            Network::Tethys => "0xCD8e983F8c4202B0085825Cf21833927D1e2b6Dc".parse().unwrap(),
            Network::Mainnet => "0x36E2B147Db67E76aB67a4d07C293670EbeFcAE4E".parse().unwrap(),
        }
    }

    pub fn network_controller_default_addr(&self) -> Address {
        match self {
            Network::Tethys => "0x68Fc7E375945d8C8dFb0050c337Ff09E962D976D".parse().unwrap(),
            Network::Mainnet => "0x4cf58097D790B193D22ed633bF8b15c9bc4F0da7".parse().unwrap(),
        }
    }

    pub fn allocations_viewer_default_addr(&self) -> Address {
        match self {
            Network::Tethys => "0xC0Af6432947db51e0C179050dAF801F19d40D2B7".parse().unwrap(),
            Network::Mainnet => "0x88CE6D8D70df9Fe049315fd9D6c3d59108C15c4C".parse().unwrap(),
        }
    }

    pub fn multicall_default_addr(&self) -> Address {
        match self {
            Network::Tethys => "0xcA11bde05977b3631167028862bE2a173976CA11".parse().unwrap(),
            Network::Mainnet => "0xcA11bde05977b3631167028862bE2a173976CA11".parse().unwrap(),
        }
    }
}
