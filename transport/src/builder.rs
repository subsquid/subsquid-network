use std::time::Duration;

use futures_core::Stream;
use libp2p::{
    multiaddr::Protocol,
    noise,
    swarm::{dial_opts::DialOpts, NetworkBehaviour},
    yamux, StreamProtocol, Swarm, SwarmBuilder,
};

use crate::{
    behaviour::base::{BaseBehaviour, BaseConfig},
    cli::{BootNode, TransportArgs},
    util::get_keypair,
    Error, Keypair, Multiaddr, PeerId, QuicConfig,
};

#[cfg(feature = "gateway")]
use crate::actors::gateway::{
    self, GatewayBehaviour, GatewayConfig, GatewayEvent, GatewayTransportHandle,
};
#[cfg(feature = "logs-collector")]
use crate::actors::logs_collector::{
    self, LogsCollectorBehaviour, LogsCollectorConfig, LogsCollectorEvent,
    LogsCollectorTransportHandle,
};
#[cfg(feature = "observer")]
use crate::actors::observer::{
    self, ObserverBehaviour, ObserverConfig, ObserverEvent, ObserverTransportHandle,
};
#[cfg(feature = "pings-collector")]
use crate::actors::pings_collector::{
    self, Ping, PingsCollectorBehaviour, PingsCollectorConfig, PingsCollectorTransportHandle,
};
#[cfg(feature = "scheduler")]
use crate::actors::scheduler::{
    self, SchedulerBehaviour, SchedulerConfig, SchedulerEvent, SchedulerTransportHandle,
};
#[cfg(feature = "worker")]
use crate::actors::worker::{
    self, WorkerBehaviour, WorkerConfig, WorkerEvent, WorkerTransportHandle,
};
use crate::protocol::dht_protocol;

pub struct P2PTransportBuilder {
    keypair: Keypair,
    listen_addrs: Vec<Multiaddr>,
    public_addrs: Vec<Multiaddr>,
    boot_nodes: Vec<BootNode>,
    relay_addrs: Vec<Multiaddr>,
    relay: bool,
    quic_config: QuicConfig,
    base_config: BaseConfig,
    contract_client: Box<dyn contract_client::Client>,
    dht_protocol: StreamProtocol,
}
impl P2PTransportBuilder {
    pub async fn from_cli(args: TransportArgs) -> anyhow::Result<Self> {
        let listen_addrs = args.listen_addrs();
        let keypair = get_keypair(args.key).await?;
        let contract_client = contract_client::get_client(&args.rpc).await?;
        let dht_protocol = dht_protocol(args.rpc.network);
        Ok(Self {
            keypair,
            listen_addrs,
            public_addrs: args.p2p_public_addrs,
            boot_nodes: args.boot_nodes,
            relay_addrs: vec![],
            relay: false,
            quic_config: QuicConfig::from_env(),
            base_config: Default::default(),
            contract_client,
            dht_protocol,
        })
    }

    pub fn with_listen_addrs<I: IntoIterator<Item = Multiaddr>>(mut self, addrs: I) -> Self {
        self.listen_addrs.extend(addrs);
        self
    }

    pub fn with_public_addrs<I: IntoIterator<Item = Multiaddr>>(mut self, addrs: I) -> Self {
        self.public_addrs.extend(addrs);
        self
    }

    pub fn with_boot_nodes<I: IntoIterator<Item = BootNode>>(mut self, nodes: I) -> Self {
        self.boot_nodes.extend(nodes);
        self
    }

    pub fn with_relay(mut self, relay: bool) -> Self {
        self.relay = relay;
        self
    }

    pub fn with_relay_addrs<I: IntoIterator<Item = Multiaddr>>(mut self, addrs: I) -> Self {
        self.relay_addrs.extend(addrs);
        self.relay = true;
        self
    }

    pub fn with_quic_config(mut self, f: impl FnOnce(QuicConfig) -> QuicConfig) -> Self {
        self.quic_config = f(self.quic_config);
        self
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.keypair.public().to_peer_id()
    }

    pub fn keypair(&self) -> Keypair {
        self.keypair.clone()
    }

    pub fn contract_client(&self) -> Box<dyn contract_client::Client> {
        self.contract_client.clone_client()
    }

    fn build_swarm<T: NetworkBehaviour>(
        mut self,
        behaviour: impl FnOnce(BaseBehaviour) -> T,
    ) -> Result<Swarm<T>, Error> {
        let mut swarm = SwarmBuilder::with_existing_identity(self.keypair)
            .with_tokio()
            .with_quic_config(|config| {
                let mut config = config.mtu_upper_bound(self.quic_config.mtu_discovery_max);
                config.keep_alive_interval =
                    Duration::from_millis(self.quic_config.keep_alive_interval_ms as u64);
                config.max_idle_timeout = self.quic_config.max_idle_timeout_ms;
                config
            })
            .with_dns()?
            .with_relay_client(noise::Config::new, yamux::Config::default)?
            .with_behaviour(|keypair, relay| {
                let base = BaseBehaviour::new(
                    keypair,
                    self.contract_client,
                    self.base_config,
                    self.boot_nodes.clone(),
                    relay,
                    self.dht_protocol,
                );
                behaviour(base)
            })
            .expect("infallible")
            .build();

        // If relay node not specified explicitly, use boot nodes
        if self.relay && self.relay_addrs.is_empty() {
            self.relay_addrs = self
                .boot_nodes
                .iter()
                .map(|bn| bn.address.clone().with(Protocol::P2p(bn.peer_id)))
                .collect();
        }

        // Listen on provided addresses
        for addr in self.listen_addrs {
            swarm.listen_on(addr)?;
        }

        // Register public addresses
        for addr in self.public_addrs {
            swarm.add_external_address(addr);
        }

        // Connect to boot nodes
        for BootNode { peer_id, address } in self.boot_nodes {
            log::info!("Connecting to boot node {peer_id} at {address}");
            swarm.dial(DialOpts::peer_id(peer_id).addresses(vec![address]).build())?;
        }

        // Connect to relay and listen for relayed connections
        if self.relay {
            for addr in self.relay_addrs {
                log::info!("Connecting to relay {addr}");
                swarm.listen_on(addr.with(Protocol::P2pCircuit))?;
            }
        }

        Ok(swarm)
    }

    #[cfg(feature = "gateway")]
    pub fn build_gateway(
        self,
        config: GatewayConfig,
    ) -> Result<(impl Stream<Item = GatewayEvent>, GatewayTransportHandle), Error> {
        let swarm = self.build_swarm(|base| GatewayBehaviour::new(base, config))?;
        Ok(gateway::start_transport(swarm, config))
    }

    #[cfg(feature = "logs-collector")]
    pub fn build_logs_collector(
        self,
        config: LogsCollectorConfig,
    ) -> Result<(impl Stream<Item = LogsCollectorEvent>, LogsCollectorTransportHandle), Error> {
        let swarm = self.build_swarm(|base| LogsCollectorBehaviour::new(base, config))?;
        Ok(logs_collector::start_transport(swarm, config))
    }

    #[cfg(feature = "observer")]
    pub fn build_observer(
        self,
        config: ObserverConfig,
    ) -> Result<(impl Stream<Item = ObserverEvent>, ObserverTransportHandle), Error> {
        let swarm =
            self.build_swarm(|base| ObserverBehaviour::new(base, config.logs_collector_id))?;
        Ok(observer::start_transport(swarm, config))
    }

    #[cfg(feature = "pings-collector")]
    pub fn build_pings_collector(
        self,
        config: PingsCollectorConfig,
    ) -> Result<(impl Stream<Item = Ping>, PingsCollectorTransportHandle), Error> {
        let swarm = self.build_swarm(|base| PingsCollectorBehaviour::new(base))?;
        Ok(pings_collector::start_transport(swarm, config))
    }

    #[cfg(feature = "scheduler")]
    pub fn build_scheduler(
        self,
        config: SchedulerConfig,
    ) -> Result<(impl Stream<Item = SchedulerEvent>, SchedulerTransportHandle), Error> {
        let swarm = self.build_swarm(|base| SchedulerBehaviour::new(base, config))?;
        Ok(scheduler::start_transport(swarm, config))
    }

    #[cfg(feature = "worker")]
    pub async fn build_worker(
        self,
        config: WorkerConfig,
    ) -> Result<(impl Stream<Item = WorkerEvent>, WorkerTransportHandle), Error> {
        let local_peer_id = self.local_peer_id();
        // Wait for the worker to be registered on chain
        loop {
            let Some(reg_time) =
                self.contract_client.worker_registration_time(local_peer_id).await?
            else {
                log::info!("Waiting for worker {local_peer_id} to be registered on chain...");
                tokio::time::sleep(Duration::from_secs(60)).await;
                continue;
            };
            // We need to wait for other nodes to catch up and whitelist our peer ID
            let elapsed = reg_time.elapsed().unwrap_or_default();
            if elapsed < self.base_config.nodes_update_interval {
                tokio::time::sleep(self.base_config.nodes_update_interval - elapsed).await;
            }
            break;
        }
        let swarm = self.build_swarm(|base| WorkerBehaviour::new(base, local_peer_id, config))?;
        Ok(worker::start_transport(swarm, config))
    }
}
