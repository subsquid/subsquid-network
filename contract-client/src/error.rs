use ethers::{
    contract::{ContractError, MulticallError},
    prelude::{AbiError, Middleware},
};
use libp2p::identity::ParseError;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Invalid RPC URL: {0:?}")]
    InvalidRpcUrl(#[from] url::ParseError),
    #[error("Websocket client error: {0}")]
    WsClient(#[from] ethers::providers::WsClientError),
    #[error("Invalid Peer ID: {0:?}")]
    InvalidPeerId(#[from] ParseError),
    #[error("Contract error: {0}")]
    Contract(String),
    #[error("RPC provider error: {0}")]
    Provider(#[from] ethers::providers::ProviderError),
    #[error("Unsupported RPC protocol")]
    InvalidProtocol,
    #[error("Transaction receipt missing")]
    TxReceiptMissing,
    #[error("Block not found")]
    BlockNotFound,
}

impl<M: Middleware> From<ContractError<M>> for ClientError {
    fn from(err: ContractError<M>) -> Self {
        Self::Contract(err.to_string())
    }
}

impl<M: Middleware> From<MulticallError<M>> for ClientError {
    fn from(err: MulticallError<M>) -> Self {
        Self::Contract(err.to_string())
    }
}

impl From<AbiError> for ClientError {
    fn from(err: AbiError) -> Self {
        Self::Contract(err.to_string())
    }
}
