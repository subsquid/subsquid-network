use contract_client::Network;
use libp2p::StreamProtocol;

pub const PING_TOPIC: &str = "/subsquid/worker_pings/0.0.1";
pub const LOGS_TOPIC: &str = "/subsquid/worker_query_logs/0.0.1";
pub const ID_PROTOCOL: StreamProtocol = StreamProtocol::new("/subsquid/0.0.1");
pub const QUERY_PROTOCOL: &str = "/subsquid/query/0.0.1";
pub const GATEWAY_LOGS_PROTOCOL: &str = "/subsquid/gateway-logs/0.0.1";
pub const WORKER_LOGS_PROTOCOL: &str = "/subsquid/worker-logs/0.0.1";
pub const PONG_PROTOCOL: &str = "/subsquid/pong/0.0.1";

pub const MAX_QUERY_SIZE: u64 = 1024 * 1024;
pub const MAX_QUERY_RESULT_SIZE: u64 = 100 * 1024 * 1024;
pub const MAX_GATEWAY_LOG_SIZE: u64 = 1024 * 1024;
pub const MAX_WORKER_LOGS_SIZE: u64 = 1024 * 1024;
pub const MAX_PONG_SIZE: u64 = 1024 * 1024;

pub const fn dht_protocol(network: Network) -> StreamProtocol {
    match network {
        Network::Tethys => StreamProtocol::new("/subsquid/dht/tethys/1.0.0"),
        Network::Mainnet => StreamProtocol::new("/subsquid/dht/mainnet/1.0.0"),
    }
}
