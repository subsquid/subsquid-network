use std::{
    collections::{HashMap, HashSet},
    iter::zip,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use ethers::prelude::{BlockId, Bytes, Middleware, Multicall, Provider};
use libp2p::futures::Stream;
use tokio_stream::{wrappers::IntervalStream, StreamExt};

use crate::{
    contracts,
    contracts::{
        AllocationsViewer, GatewayRegistry, NetworkController, Strategy, WorkerRegistration,
    },
    transport::Transport,
    Address, ClientError, PeerId, RpcArgs, U256,
};

const GATEWAYS_PAGE_SIZE: U256 = U256([10000, 0, 0, 0]);

#[derive(Debug, Clone)]
pub struct Allocation {
    pub worker_peer_id: PeerId,
    pub worker_onchain_id: U256,
    pub computation_units: U256,
}

#[derive(Debug, Clone)]
pub struct GatewayCluster {
    pub operator_addr: Address,
    pub gateway_ids: Vec<PeerId>,
    pub allocated_computation_units: U256,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Worker {
    pub peer_id: PeerId,
    pub onchain_id: U256,
    pub address: Address,
    pub bond: U256,
    pub registered_at: u128,
    pub deregistered_at: Option<u128>,
}

impl Worker {
    fn new(worker: contracts::Worker, onchain_id: U256) -> Result<Self, ClientError> {
        let peer_id = PeerId::from_bytes(&worker.peer_id)?;
        let deregistered_at = (worker.deregistered_at > 0).then_some(worker.deregistered_at);
        Ok(Self {
            peer_id,
            onchain_id,
            address: worker.creator,
            bond: worker.bond,
            registered_at: worker.registered_at,
            deregistered_at,
        })
    }
}

pub type NodeStream =
    Pin<Box<dyn Stream<Item = Result<HashSet<PeerId>, ClientError>> + Send + 'static>>;

#[async_trait]
pub trait Client: Send + Sync + 'static {
    /// Using regular clone is not possible for trait objects
    fn clone_client(&self) -> Box<dyn Client>;

    /// Get the current epoch number
    async fn current_epoch(&self) -> Result<u32, ClientError>;

    /// Get the time when the current epoch started
    async fn current_epoch_start(&self) -> Result<SystemTime, ClientError>;

    /// Get the on-chain ID for the worker
    async fn worker_id(&self, peer_id: PeerId) -> Result<U256, ClientError>;

    /// Get current active worker set
    async fn active_workers(&self) -> Result<Vec<Worker>, ClientError>;

    /// Check if gateway (client) is registered on chain
    async fn is_gateway_registered(&self, peer_id: PeerId) -> Result<bool, ClientError>;

    /// Get current active gateways
    async fn active_gateways(&self) -> Result<Vec<PeerId>, ClientError>;

    /// Get client's allocations for the current epoch.
    async fn current_allocations(
        &self,
        client_id: PeerId,
        worker_ids: Option<Vec<Worker>>,
    ) -> Result<Vec<Allocation>, ClientError>;

    /// Get the current list of all gateway clusters with their allocated CUs
    async fn gateway_clusters(&self, worker_id: U256) -> Result<Vec<GatewayCluster>, ClientError>;

    /// Get a stream of peer IDs of all active network participants (workers & gateways)
    /// Updated on the given interval
    fn network_nodes_stream(self: Box<Self>, interval: Duration) -> NodeStream {
        Box::pin(IntervalStream::new(tokio::time::interval(interval)).then(move |_| {
            let client = self.clone_client();
            async move {
                let gateways = client.active_gateways().await?;
                let workers = client.active_workers().await?;
                let mut nodes = HashSet::from_iter(gateways);
                nodes.extend(workers.into_iter().map(|w| w.peer_id));
                Ok(nodes)
            }
        }))
    }
}

pub async fn get_client(rpc_args: &RpcArgs) -> Result<Box<dyn Client>, ClientError> {
    let l2_client = Transport::connect(&rpc_args.rpc_url).await?;
    let l1_client = match &rpc_args.l1_rpc_url {
        Some(rpc_url) => Transport::connect(rpc_url).await?,
        None => {
            log::warn!("Layer 1 RPC URL not provided. Assuming the main RPC URL is L1");
            l2_client.clone()
        }
    };
    let client: Box<dyn Client> = EthersClient::new(l1_client, l2_client, rpc_args).await?;
    Ok(client)
}

#[derive(Clone)]
struct EthersClient {
    l1_client: Arc<Provider<Transport>>,
    l2_client: Arc<Provider<Transport>>,
    gateway_registry: GatewayRegistry<Provider<Transport>>,
    network_controller: NetworkController<Provider<Transport>>,
    worker_registration: WorkerRegistration<Provider<Transport>>,
    allocations_viewer: AllocationsViewer<Provider<Transport>>,
    default_strategy_addr: Address,
    multicall_contract_addr: Option<Address>,
}

impl EthersClient {
    pub async fn new(
        l1_client: Arc<Provider<Transport>>,
        l2_client: Arc<Provider<Transport>>,
        rpc_args: &RpcArgs,
    ) -> Result<Box<Self>, ClientError> {
        let gateway_registry =
            GatewayRegistry::get(l2_client.clone(), rpc_args.gateway_registry_addr());
        let default_strategy_addr = gateway_registry.default_strategy().call().await?;
        let network_controller =
            NetworkController::get(l2_client.clone(), rpc_args.network_controller_addr());
        let worker_registration =
            WorkerRegistration::get(l2_client.clone(), rpc_args.worker_registration_addr());
        let allocations_viewer =
            AllocationsViewer::get(l2_client.clone(), rpc_args.allocations_viewer_addr());
        Ok(Box::new(Self {
            l1_client,
            l2_client,
            gateway_registry,
            worker_registration,
            network_controller,
            allocations_viewer,
            default_strategy_addr,
            multicall_contract_addr: Some(rpc_args.multicall_addr()),
        }))
    }

    async fn multicall(&self) -> Result<Multicall<Provider<Transport>>, ClientError> {
        Ok(contracts::multicall(self.l2_client.clone(), self.multicall_contract_addr).await?)
    }
}

#[async_trait]
impl Client for EthersClient {
    fn clone_client(&self) -> Box<dyn Client> {
        Box::new(self.clone())
    }

    async fn current_epoch(&self) -> Result<u32, ClientError> {
        let epoch = self
            .network_controller
            .epoch_number()
            .call()
            .await?
            .try_into()
            .expect("Epoch number should not exceed u32 range");
        Ok(epoch)
    }

    async fn current_epoch_start(&self) -> Result<SystemTime, ClientError> {
        let next_epoch_start_block = self.network_controller.next_epoch().call().await?;
        let epoch_length_blocks = self.network_controller.epoch_length().call().await?;
        let block_num: u64 = (next_epoch_start_block - epoch_length_blocks)
            .try_into()
            .expect("Epoch number should not exceed u64 range");
        log::debug!("Current epoch: {block_num} Epoch length: {epoch_length_blocks} Next epoch: {next_epoch_start_block}");
        // Blocks returned by `next_epoch()` and `epoch_length()` are **L1 blocks**
        let block = self
            .l1_client
            .get_block(BlockId::Number(block_num.into()))
            .await?
            .ok_or(ClientError::BlockNotFound)?;
        Ok(UNIX_EPOCH + Duration::from_secs(block.timestamp.as_u64()))
    }

    async fn worker_id(&self, peer_id: PeerId) -> Result<U256, ClientError> {
        let peer_id = peer_id.to_bytes().into();
        let id: U256 = self.worker_registration.worker_ids(peer_id).call().await?;
        Ok(id)
    }

    async fn active_workers(&self) -> Result<Vec<Worker>, ClientError> {
        let workers_call = self.worker_registration.method("getActiveWorkers", ())?;
        let onchain_ids_call = self.worker_registration.method("getActiveWorkerIds", ())?;
        let mut multicall = self.multicall().await?;
        multicall
            .add_call::<Vec<contracts::Worker>>(workers_call, false)
            .add_call::<Vec<U256>>(onchain_ids_call, false);
        let (workers, onchain_ids): (Vec<contracts::Worker>, Vec<U256>) = multicall.call().await?;

        let workers = workers
            .into_iter()
            .zip(onchain_ids)
            .filter_map(|(worker, onchain_id)| match Worker::new(worker, onchain_id) {
                Ok(worker) => Some(worker),
                Err(e) => {
                    log::debug!("Error reading worker from chain: {e:?}");
                    None
                }
            })
            .collect();
        Ok(workers)
    }

    async fn is_gateway_registered(&self, peer_id: PeerId) -> Result<bool, ClientError> {
        let gateway_id = peer_id.to_bytes().into();
        let gateway_info: contracts::Gateway =
            self.gateway_registry.get_gateway(gateway_id).call().await?;
        Ok(gateway_info.operator != Address::zero())
    }

    async fn active_gateways(&self) -> Result<Vec<PeerId>, ClientError> {
        let latest_block = self.l2_client.get_block_number().await?;
        let mut active_gateways = Vec::new();
        for page in 0.. {
            let gateway_ids = self
                .gateway_registry
                .get_active_gateways(page.into(), GATEWAYS_PAGE_SIZE)
                .block(latest_block)
                .call()
                .await?;
            let page_size = U256::from(gateway_ids.len());

            active_gateways.extend(gateway_ids.iter().filter_map(|id| PeerId::from_bytes(id).ok()));
            if page_size < GATEWAYS_PAGE_SIZE {
                break;
            }
        }
        Ok(active_gateways)
    }

    async fn current_allocations(
        &self,
        client_id: PeerId,
        workers: Option<Vec<Worker>>,
    ) -> Result<Vec<Allocation>, ClientError> {
        let workers = match workers {
            Some(workers) => workers,
            None => self.active_workers().await?,
        };
        if workers.is_empty() {
            return Ok(vec![]);
        }

        let gateway_id: Bytes = client_id.to_bytes().into();
        let strategy_addr =
            self.gateway_registry.get_used_strategy(gateway_id.clone()).call().await?;
        let strategy = Strategy::get(strategy_addr, self.l2_client.clone());

        // A little hack to make less requests: default strategy distributes CUs evenly,
        // so we can just query for one worker and return the same number for all.
        if strategy_addr == self.default_strategy_addr {
            let first_worker_id = workers.first().expect("non empty").onchain_id;
            let cus_per_epoch =
                strategy.computation_units_per_epoch(gateway_id, first_worker_id).call().await?;
            return Ok(workers
                .into_iter()
                .map(|w| Allocation {
                    worker_peer_id: w.peer_id,
                    worker_onchain_id: w.onchain_id,
                    computation_units: cus_per_epoch,
                })
                .collect());
        }

        let mut multicall = self.multicall().await?;
        for worker in workers.iter() {
            multicall.add_call::<U256>(
                strategy
                    .method("computationUnitsPerEpoch", (gateway_id.clone(), worker.onchain_id))?,
                false,
            );
        }
        let compute_units: Vec<U256> = multicall.call_array().await?;
        Ok(zip(workers, compute_units)
            .map(|(w, cus)| Allocation {
                worker_peer_id: w.peer_id,
                worker_onchain_id: w.onchain_id,
                computation_units: cus,
            })
            .collect())
    }

    async fn gateway_clusters(&self, worker_id: U256) -> Result<Vec<GatewayCluster>, ClientError> {
        let latest_block = self.l2_client.get_block_number().await?;

        let mut clusters = HashMap::new();
        for page in 0.. {
            let allocations = self
                .allocations_viewer
                .get_allocations(worker_id, page.into(), GATEWAYS_PAGE_SIZE)
                .block(latest_block)
                .call()
                .await?;
            let page_size = U256::from(allocations.len());

            for allocation in allocations {
                let gateway_peer_id = match PeerId::from_bytes(&allocation.gateway_id) {
                    Ok(peer_id) => peer_id,
                    _ => continue,
                };
                clusters
                    .entry(allocation.operator)
                    .or_insert_with(|| GatewayCluster {
                        operator_addr: allocation.operator,
                        gateway_ids: Vec::new(),
                        allocated_computation_units: allocation.allocated,
                    })
                    .gateway_ids
                    .push(gateway_peer_id);
            }

            if page_size < GATEWAYS_PAGE_SIZE {
                break;
            }
        }
        Ok(clusters.into_values().collect())
    }
}
