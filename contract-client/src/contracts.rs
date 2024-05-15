use ethers::{
    contract::abigen,
    prelude::{Multicall, MulticallError},
    providers::Middleware,
};
use std::sync::Arc;

use crate::Address;

pub use gateway_registry::Gateway;
pub use worker_registration::Worker;

abigen!(GatewayRegistry, "abi/GatewayRegistry.json");
abigen!(NetworkController, "abi/NetworkController.json");
abigen!(Strategy, "abi/Strategy.json");
abigen!(WorkerRegistration, "abi/WorkerRegistration.json");
abigen!(AllocationsViewer, "abi/AllocationsViewer.json");

impl<T: Middleware> GatewayRegistry<T> {
    pub fn get(client: Arc<T>, addr: Address) -> Self {
        Self::new(addr, client)
    }
}

impl<T: Middleware> WorkerRegistration<T> {
    pub fn get(client: Arc<T>, addr: Address) -> Self {
        Self::new(addr, client)
    }
}

impl<T: Middleware> NetworkController<T> {
    pub fn get(client: Arc<T>, addr: Address) -> Self {
        Self::new(addr, client)
    }
}

impl<T: Middleware> AllocationsViewer<T> {
    pub fn get(client: Arc<T>, addr: Address) -> Self {
        Self::new(addr, client)
    }
}

impl<T: Middleware> Strategy<T> {
    pub fn get(addr: impl Into<Address>, client: Arc<T>) -> Self {
        Self::new(addr, client)
    }
}

pub async fn multicall<T: Middleware>(
    client: Arc<T>,
    addr: Option<Address>,
) -> Result<Multicall<T>, MulticallError<T>> {
    Multicall::new(client, addr).await
}
