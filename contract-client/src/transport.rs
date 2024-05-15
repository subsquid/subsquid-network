use crate::ClientError;
use ethers::{
    prelude::{Http, JsonRpcClient, JsonRpcError, Provider, ProviderError, RpcError, Ws},
    utils::__serde_json::Error,
};
use libp2p::futures::TryFutureExt;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt::{Debug, Display, Formatter},
    future::Future,
    pin::Pin,
    sync::Arc,
};
use url::Url;

#[derive(Debug, Clone)]
pub enum Transport {
    Http(Http),
    Ws(Ws),
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    Http(#[from] ethers::providers::HttpClientError),
    Ws(#[from] ethers::providers::WsClientError),
}

impl Display for TransportError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::Http(e) => Display::fmt(e, f),
            TransportError::Ws(e) => Display::fmt(e, f),
        }
    }
}

impl RpcError for TransportError {
    fn as_error_response(&self) -> Option<&JsonRpcError> {
        match self {
            TransportError::Http(e) => e.as_error_response(),
            TransportError::Ws(e) => e.as_error_response(),
        }
    }

    fn as_serde_error(&self) -> Option<&Error> {
        match self {
            TransportError::Http(e) => e.as_serde_error(),
            TransportError::Ws(e) => e.as_serde_error(),
        }
    }
}

impl Into<ProviderError> for TransportError {
    fn into(self) -> ProviderError {
        match self {
            TransportError::Http(e) => e.into(),
            TransportError::Ws(e) => e.into(),
        }
    }
}

impl Transport {
    pub async fn connect(rpc_url: &str) -> Result<Arc<Provider<Self>>, ClientError> {
        let transport = if rpc_url.starts_with("http") {
            Transport::Http(Http::new(Url::parse(rpc_url)?))
        } else if rpc_url.starts_with("ws") {
            Transport::Ws(Ws::connect(rpc_url).await?)
        } else {
            return Err(ClientError::InvalidProtocol);
        };
        Ok(Arc::new(Provider::new(transport)))
    }
}

impl JsonRpcClient for Transport {
    type Error = TransportError;

    fn request<'life0, 'life1, 'async_trait, T, R>(
        &'life0 self,
        method: &'life1 str,
        params: T,
    ) -> Pin<Box<dyn Future<Output = Result<R, Self::Error>> + Send + 'async_trait>>
    where
        T: Debug + Serialize + Send + Sync,
        R: DeserializeOwned + Send,
        T: 'async_trait,
        R: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Transport::Http(provider) => {
                Box::pin(provider.request(method, params).map_err(Into::into))
            }
            Transport::Ws(provider) => {
                Box::pin(provider.request(method, params).map_err(Into::into))
            }
        }
    }
}
