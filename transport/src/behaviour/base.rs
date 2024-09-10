use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
    vec,
};

use bimap::BiHashMap;
use futures::StreamExt;
use futures_bounded::FuturesMap;
use libp2p::{
    allow_block_list,
    allow_block_list::AllowedPeers,
    autonat,
    autonat::NatStatus,
    core::ConnectedPoint,
    dcutr, identify,
    identity::Keypair,
    kad,
    kad::{
        store::MemoryStore, GetClosestPeersError, GetClosestPeersOk, ProgressStep, QueryId,
        QueryResult,
    },
    ping, relay,
    swarm::{
        behaviour::ConnectionEstablished,
        dial_opts::{DialOpts, PeerCondition},
        ConnectionClosed, DialFailure, FromSwarm, NetworkBehaviour, ToSwarm,
    },
    StreamProtocol,
};
use libp2p_swarm_derive::NetworkBehaviour;
use parking_lot::RwLock;
use prost::Message;
use serde::{Deserialize, Serialize};

use contract_client::{NetworkNodes, NodeStream};
use subsquid_messages::{
    signatures::SignedMessage, worker_logs_msg, LogsCollected, Ping, QueryExecuted, QueryLogs,
    WorkerLogsMsg,
};

#[cfg(feature = "metrics")]
use crate::metrics::{ACTIVE_CONNECTIONS, ONGOING_PROBES, ONGOING_QUERIES};
use crate::{
    behaviour::{
        addr_cache::AddressCache,
        pubsub::{MsgValidationConfig, PubsubBehaviour, PubsubMsg, ValidationError},
        wrapped::{BehaviourWrapper, TToSwarm, Wrapped},
    },
    cli::BootNode,
    protocol::{
        ID_PROTOCOL, KEEP_LAST_WORKER_LOGS, LOGS_COLLECTED_MIN_INTERVAL, LOGS_COLLECTED_TOPIC,
        LOGS_MIN_INTERVAL, MAX_PUBSUB_MSG_SIZE, PINGS_MIN_INTERVAL, PING_TOPIC, WORKER_LOGS_TOPIC,
    },
    record_event,
    util::{addr_is_reachable, parse_env_var},
    PeerId, QueueFull,
};

#[derive(NetworkBehaviour)]
pub struct InnerBehaviour {
    identify: identify::Behaviour,
    kademlia: kad::Behaviour<MemoryStore>,
    relay: relay::client::Behaviour,
    dcutr: dcutr::Behaviour,
    ping: ping::Behaviour,
    autonat: autonat::Behaviour,
    allow: allow_block_list::Behaviour<AllowedPeers>,
    pubsub: Wrapped<PubsubBehaviour>,
    address_cache: AddressCache,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BaseConfig {
    /// How often to check for whitelisted nodes on chain (default: 5 min).
    pub nodes_update_interval: Duration,
    /// Timeout for autoNAT probes (default: 60 sec).
    pub autonat_timeout: Duration,
    /// How often to publish indetify info to connected nodes (default: 60 sec).
    pub identify_interval: Duration,
    /// Timeout for outgoing reachability probes (default: 20 sec).
    pub probe_timeout: Duration,
    /// Timeout for kademlia DHT queries (default: 10 sec).
    pub kad_query_timeout: Duration,
    /// Maximum number of concurrent outgoing reachability probes (default: 1024)
    pub max_concurrent_probes: usize,
    /// Maximum size of gossipsub messages in bytes (default: `MAX_PUBSUB_MSG_SIZE`)
    pub max_pubsub_msg_size: usize,
    /// Maximum number of peers to keep in the address cache (default: 1024)
    pub addr_cache_size: NonZeroUsize,
}

impl BaseConfig {
    pub fn from_env() -> Self {
        let nodes_update_interval =
            Duration::from_secs(parse_env_var("NODES_UPDATE_INTERVAL_SEC", 300));
        let autonat_timeout = Duration::from_secs(parse_env_var("AUTONAT_TIMEOUT_SEC", 60));
        let identify_interval = Duration::from_secs(parse_env_var("IDENTIFY_INTERVAL_SEC", 60));
        let probe_timeout = Duration::from_secs(parse_env_var("PROBE_TIMEOUT_SEC", 20));
        let kad_query_timeout = Duration::from_secs(parse_env_var("KAD_QUERY_TIMEOUT_SEC", 5));
        let max_concurrent_probes = parse_env_var("MAX_CONCURRENT_PROBES", 1024);
        let max_pubsub_msg_size = parse_env_var("MAX_PUBSUB_MSG_SIZE", MAX_PUBSUB_MSG_SIZE);
        let addr_cache_size = NonZeroUsize::new(parse_env_var("ADDR_CACHE_SIZE", 1024))
            .expect("addr_cache_size should be > 0");
        Self {
            nodes_update_interval,
            autonat_timeout,
            identify_interval,
            probe_timeout,
            kad_query_timeout,
            max_concurrent_probes,
            max_pubsub_msg_size,
            addr_cache_size,
        }
    }
}

pub struct BaseBehaviour {
    inner: InnerBehaviour,
    keypair: Keypair,
    ongoing_queries: BiHashMap<PeerId, QueryId>,
    outbound_conns: HashMap<PeerId, u32>,
    probe_timeouts: FuturesMap<PeerId, ()>,
    registered_nodes: HashSet<PeerId>,
    registered_workers: Arc<RwLock<HashSet<PeerId>>>,
    logs_collected: Arc<RwLock<HashMap<String, u64>>>, // peer_id (base58) -> highest collected seq_no
    active_nodes_stream: NodeStream,
    max_pubsub_msg_size: usize,
}

#[allow(dead_code)]
impl BaseBehaviour {
    pub fn new(
        keypair: &Keypair,
        contract_client: Box<dyn contract_client::Client>,
        config: BaseConfig,
        boot_nodes: Vec<BootNode>,
        relay: relay::client::Behaviour,
        dht_protocol: StreamProtocol,
    ) -> Self {
        let local_peer_id = keypair.public().to_peer_id();
        let mut kad_config = kad::Config::new(dht_protocol);
        kad_config.set_query_timeout(config.kad_query_timeout);
        let mut inner = InnerBehaviour {
            identify: identify::Behaviour::new(
                identify::Config::new(ID_PROTOCOL.to_string(), keypair.public())
                    .with_interval(config.identify_interval)
                    .with_push_listen_addr_updates(true),
            ),
            kademlia: kad::Behaviour::with_config(
                local_peer_id,
                MemoryStore::new(local_peer_id),
                kad_config,
            ),
            relay,
            dcutr: dcutr::Behaviour::new(local_peer_id),
            ping: ping::Behaviour::new(ping::Config::default()),
            autonat: autonat::Behaviour::new(
                local_peer_id,
                autonat::Config {
                    timeout: config.autonat_timeout,
                    ..Default::default()
                },
            ),
            allow: Default::default(),
            pubsub: PubsubBehaviour::new(keypair.clone(), config.max_pubsub_msg_size).into(),
            address_cache: AddressCache::new(config.addr_cache_size),
        };

        for boot_node in boot_nodes {
            inner.allow.allow_peer(boot_node.peer_id);
            inner.autonat.add_server(boot_node.peer_id, Some(boot_node.address));
        }

        Self {
            inner,
            keypair: keypair.clone(),
            ongoing_queries: Default::default(),
            outbound_conns: Default::default(),
            probe_timeouts: FuturesMap::new(config.probe_timeout, config.max_concurrent_probes),
            registered_nodes: Default::default(),
            registered_workers: Arc::new(RwLock::new(Default::default())),
            logs_collected: Arc::new(RwLock::new(Default::default())),
            active_nodes_stream: contract_client.network_nodes_stream(config.nodes_update_interval),
            max_pubsub_msg_size: config.max_pubsub_msg_size,
        }
    }

    pub fn subscribe_pings(&mut self) {
        let registered_workers = self.registered_workers.clone();
        let config = MsgValidationConfig::new(PINGS_MIN_INTERVAL).max_burst(2).msg_validator(
            move |peer_id: PeerId, _seq_no: u64, _data: &[u8]| {
                if !registered_workers.read().contains(&peer_id) {
                    return Err(ValidationError::Invalid("Worker not registered"));
                }
                Ok(())
            },
        );
        self.inner.pubsub.subscribe(PING_TOPIC, config);
    }

    pub fn subscribe_worker_logs(&mut self, logs_collector_id: PeerId) {
        // Unordered messages need to be allowed, because we're interested in all messages from
        // each worker, not only the most recent one (as in the case of pings).
        let registered_workers = self.registered_workers.clone();
        let logs_collected = self.logs_collected.clone();
        let config = MsgValidationConfig::new(LOGS_MIN_INTERVAL)
            .max_burst(10)
            .keep_last(KEEP_LAST_WORKER_LOGS)
            .msg_validator(move |peer_id: PeerId, _seq_no: u64, msg: &[u8]| {
                if !registered_workers.read().contains(&peer_id) {
                    return Err(ValidationError::Invalid("Worker not registered"));
                }
                let Ok(WorkerLogsMsg {
                    msg: Some(worker_logs_msg::Msg::QueryLogs(msg)),
                }) = WorkerLogsMsg::decode(msg)
                else {
                    return Err(ValidationError::Invalid("Invalid worker logs"));
                };
                // Logs are sorted by seq_no, so we need to check the last one only
                let last_seq_no = match msg.queries_executed.last() {
                    Some(query_executed) => query_executed.seq_no.unwrap_or_default(),
                    None => return Err(ValidationError::Invalid("Empty worker logs")),
                };
                // Don't propagate worker logs which have already been collected
                match logs_collected.read().get(&peer_id.to_base58()) {
                    Some(seq_no) if *seq_no >= last_seq_no => {
                        Err(ValidationError::Ignored("Logs already collected"))
                    }
                    _ => Ok(()),
                }
            });
        self.inner.pubsub.subscribe(WORKER_LOGS_TOPIC, config);

        let config = MsgValidationConfig::new(LOGS_COLLECTED_MIN_INTERVAL).msg_validator(
            move |peer_id: PeerId, _seq_no: u64, _msg: &[u8]| {
                if peer_id == logs_collector_id {
                    Ok(())
                } else {
                    Err(ValidationError::Invalid("Invalid logs collector ID"))
                }
            },
        );
        self.inner.pubsub.subscribe(LOGS_COLLECTED_TOPIC, config);
    }

    pub fn sign<T: SignedMessage>(&self, msg: &mut T) {
        msg.sign(&self.keypair)
    }

    pub fn publish_ping(&mut self, mut ping: Ping) {
        self.sign(&mut ping);
        self.inner.pubsub.publish(PING_TOPIC, ping.encode_to_vec());
    }

    pub fn publish_worker_logs(&mut self, mut logs: Vec<QueryExecuted>) {
        for log in &mut logs {
            self.sign(log);
        }
        for bundle in bundle_messages(logs, self.max_pubsub_msg_size / 2) {
            let msg: WorkerLogsMsg = bundle.into();
            self.inner.pubsub.publish(WORKER_LOGS_TOPIC, msg.encode_to_vec());
        }
    }

    pub fn publish_logs_collected(&mut self, logs_collected: &LogsCollected) {
        self.inner.pubsub.publish(LOGS_COLLECTED_TOPIC, logs_collected.encode_to_vec());
    }

    pub fn find_and_dial(&mut self, peer_id: PeerId) {
        if self.ongoing_queries.contains_left(&peer_id) {
            log::debug!("Query for peer {peer_id} already ongoing");
        } else {
            log::debug!("Starting query for peer {peer_id}");
            let query_id = self.inner.kademlia.get_closest_peers(peer_id);
            self.ongoing_queries.insert(peer_id, query_id);
            #[cfg(feature = "metrics")]
            ONGOING_QUERIES.inc();
        }
    }

    /// Try to probe if peer is reachable. Returns:
    ///   * Ok(true) if there is an established outbound connection to peer,
    ///   * Ok(false) if a probe has been scheduled,
    ///   * Err(QueueFull) if probe cannot be scheduled.
    pub fn try_probe_peer(&mut self, peer_id: PeerId) -> Result<bool, QueueFull> {
        if self.outbound_conns.get(&peer_id).is_some_and(|x| *x > 0) {
            log::debug!("Outbound connection to {peer_id} already exists");
            return Ok(true);
        }
        if self.probe_timeouts.contains(peer_id) {
            log::debug!("Probe for peer {peer_id} already ongoing");
            return Ok(false);
        }
        if self.probe_timeouts.try_push(peer_id, futures::future::pending()).is_err() {
            return Err(QueueFull);
        }
        log::debug!("Probing peer {peer_id}");
        #[cfg(feature = "metrics")]
        ONGOING_PROBES.inc();
        self.find_and_dial(peer_id);
        Ok(false)
    }

    pub fn allow_peer(&mut self, peer_id: PeerId) {
        log::debug!("Allowing peer {peer_id}");
        self.inner.allow.allow_peer(peer_id);
    }

    fn on_nodes_update(&mut self, result: Result<NetworkNodes, contract_client::ClientError>) {
        let nodes = match result {
            Err(e) => {
                log::error!("Error retrieving registered nodes from chain: {e:?}");
                return;
            }
            Ok(nodes) => nodes,
        };
        self.registered_workers.write().clone_from(&nodes.workers);

        let nodes = nodes.all();
        if nodes == self.registered_nodes {
            log::debug!("Registered nodes set unchanged.");
            return;
        }
        log::info!("Updating registered nodes");
        // Disallow nodes which are no longer registered
        for peer_id in self.registered_nodes.difference(&nodes) {
            log::debug!("Blocking peer {peer_id}");
            self.inner.allow.disallow_peer(*peer_id);
        }
        // Allow newly registered nodes
        for peer_id in nodes.difference(&self.registered_nodes) {
            log::debug!("Allowing peer {peer_id}");
            self.inner.allow.allow_peer(*peer_id);
        }
        self.registered_nodes = nodes;
    }
}

#[derive(Debug, Clone)]
pub enum BaseBehaviourEvent {
    Ping {
        peer_id: PeerId,
        ping: Ping,
    },
    WorkerQueryLogs {
        peer_id: PeerId,
        query_logs: QueryLogs,
    },
    LogsCollected(LogsCollected),
    PeerProbed {
        peer_id: PeerId,
        reachable: bool,
    },
    PeerProtocols {
        peer_id: PeerId,
        protocols: Vec<StreamProtocol>,
    },
}

impl BehaviourWrapper for BaseBehaviour {
    type Inner = InnerBehaviour;
    type Event = BaseBehaviourEvent;

    fn inner(&mut self) -> &mut Self::Inner {
        &mut self.inner
    }

    fn on_swarm_event(&mut self, ev: FromSwarm) -> impl IntoIterator<Item = TToSwarm<Self>> {
        match ev {
            FromSwarm::ConnectionEstablished(conn) => self.on_connection_established(conn),
            FromSwarm::ConnectionClosed(conn) => self.on_connection_closed(conn),
            FromSwarm::DialFailure(DialFailure { peer_id, error, .. }) => {
                log::debug!(
                    "Failed to dial {}: {error:?}",
                    peer_id.map(PeerId::to_base58).unwrap_or_default()
                );
                None
            }
            _ => None,
        }
    }

    fn on_inner_event(
        &mut self,
        ev: <Self::Inner as NetworkBehaviour>::ToSwarm,
    ) -> impl IntoIterator<Item = TToSwarm<Self>> {
        match ev {
            InnerBehaviourEvent::Identify(ev) => self.on_identify_event(ev),
            InnerBehaviourEvent::Kademlia(ev) => self.on_kademlia_event(ev),
            InnerBehaviourEvent::Autonat(ev) => self.on_autonat_event(ev),
            InnerBehaviourEvent::Pubsub(ev) => self.on_pubsub_event(ev),
            InnerBehaviourEvent::Ping(ev) => {
                record_event(&ev);
                None
            }
            InnerBehaviourEvent::Dcutr(ev) => {
                record_event(&ev);
                None
            }
            _ => None,
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<impl IntoIterator<Item = TToSwarm<Self>>> {
        loop {
            match self.active_nodes_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(res)) => {
                    self.on_nodes_update(res);
                    continue;
                }
                Poll::Pending => {}
                _ => unreachable!(), // infinite stream
            }

            match self.probe_timeouts.poll_unpin(cx) {
                Poll::Ready((peer_id, Err(_))) => {
                    #[cfg(feature = "metrics")]
                    if ONGOING_PROBES.dec() == 0 {
                        ONGOING_PROBES.set(0); // FIXME: There is underflow sometimes
                    }
                    log::debug!("Probe for peer {peer_id} timed out");
                    return Poll::Ready(Some(ToSwarm::GenerateEvent(
                        BaseBehaviourEvent::PeerProbed {
                            peer_id,
                            reachable: false,
                        },
                    )));
                }
                Poll::Pending => {}
                _ => unreachable!(), // future::pending() should never complete
            }

            return Poll::Pending;
        }
    }
}

impl BaseBehaviour {
    fn on_connection_established(&mut self, conn: ConnectionEstablished) -> Option<TToSwarm<Self>> {
        #[cfg(feature = "metrics")]
        ACTIVE_CONNECTIONS.inc();
        let peer_id = match conn.endpoint {
            ConnectedPoint::Dialer { .. } => conn.peer_id,
            _ => return None,
        };
        log::debug!("Established outbound connection to {peer_id}");
        *self.outbound_conns.entry(peer_id).or_default() += 1;
        if self.probe_timeouts.remove(peer_id).is_some() {
            #[cfg(feature = "metrics")]
            if ONGOING_PROBES.dec() == 0 {
                ONGOING_PROBES.set(0); // FIXME: There is underflow sometimes
            }
            log::debug!("Probe for {peer_id} succeeded");
            Some(ToSwarm::GenerateEvent(BaseBehaviourEvent::PeerProbed {
                peer_id,
                reachable: true,
            }))
        } else {
            None
        }
    }

    fn on_connection_closed(&mut self, conn: ConnectionClosed) -> Option<TToSwarm<Self>> {
        #[cfg(feature = "metrics")]
        ACTIVE_CONNECTIONS.dec();
        let peer_id = match conn.endpoint {
            ConnectedPoint::Dialer { .. } => conn.peer_id,
            _ => return None,
        };
        log::debug!("Closed outbound connection to {peer_id}");
        match self.outbound_conns.get_mut(&peer_id) {
            Some(x) => *x -= 1,
            None => log::error!("Closed connection not established before"),
        }
        None
    }

    fn on_identify_event(&mut self, ev: identify::Event) -> Option<TToSwarm<Self>> {
        log::debug!("Identify event received: {ev:?}");
        record_event(&ev);
        let (peer_id, listen_addrs, protocols) = match ev {
            identify::Event::Received { peer_id, info, .. } => {
                (peer_id, info.listen_addrs, info.protocols)
            }
            _ => return None,
        };
        let listen_addrs = listen_addrs.into_iter().filter(addr_is_reachable);
        self.inner.address_cache.put(peer_id, listen_addrs.clone());
        listen_addrs.for_each(|addr| {
            self.inner.kademlia.add_address(&peer_id, addr);
        });
        let ev = BaseBehaviourEvent::PeerProtocols { peer_id, protocols };
        Some(ToSwarm::GenerateEvent(ev))
    }

    fn on_kademlia_event(&mut self, ev: kad::Event) -> Option<TToSwarm<Self>> {
        log::debug!("Kademlia event received: {ev:?}");
        record_event(&ev);
        let kad::Event::OutboundQueryProgressed {
            id: query_id,
            result: QueryResult::GetClosestPeers(result),
            step: ProgressStep { last, .. },
            ..
        } = ev
        else {
            return None;
        };

        let peer_id = self.ongoing_queries.get_by_right(&query_id)?.to_owned();
        let peer_info = match result {
            Ok(GetClosestPeersOk { peers, .. })
            | Err(GetClosestPeersError::Timeout { peers, .. }) => {
                peers.into_iter().find(|p| p.peer_id == peer_id)
            }
        };

        // Query finished
        if last || peer_info.is_some() {
            log::debug!("Query for peer {peer_id} finished.");
            self.ongoing_queries.remove_by_right(&query_id);
            #[cfg(feature = "metrics")]
            ONGOING_QUERIES.dec();
        }
        let peer_info = peer_info?;

        // Cache the found address(es) so they can be used for dialing
        // (kademlia might not do it by itself, if the bucket is full)
        self.inner.address_cache.put(peer_id, peer_info.addrs);
        Some(ToSwarm::Dial {
            // Not using the default condition (`DisconnectedAndNotDialing`), because we may want
            // to establish an outbound connection to the peer despite existing inbound connection.
            opts: DialOpts::peer_id(peer_id).condition(PeerCondition::NotDialing).build(),
        })
    }

    fn on_autonat_event(&mut self, ev: autonat::Event) -> Option<TToSwarm<Self>> {
        log::debug!("AutoNAT event received: {ev:?}");
        let autonat::Event::StatusChanged { new: status, .. } = ev else {
            return None;
        };
        match status {
            NatStatus::Public(addr) => log::info!("Public address confirmed: {addr}"),
            NatStatus::Private => log::warn!("Public address check failed."),
            NatStatus::Unknown => {}
        }
        None
    }

    fn on_pubsub_event(
        &mut self,
        PubsubMsg {
            peer_id,
            topic,
            data,
        }: PubsubMsg,
    ) -> Option<TToSwarm<Self>> {
        log::trace!("Pub-sub message received: peer_id={peer_id} topic={topic}");
        let data = data.as_ref();
        let ev = match topic {
            PING_TOPIC => decode_ping(peer_id, data)?,
            WORKER_LOGS_TOPIC => decode_worker_logs_msg(peer_id, data)?,
            LOGS_COLLECTED_TOPIC => self.on_logs_collected(data)?,
            _ => return None,
        };
        Some(ToSwarm::GenerateEvent(ev))
    }

    fn on_logs_collected(&mut self, data: &[u8]) -> Option<BaseBehaviourEvent> {
        let logs_collected = LogsCollected::decode(data)
            .map_err(|e| log::warn!("Error decoding logs collected msg: {e:?}"))
            .ok()?;
        *self.logs_collected.write() = logs_collected.sequence_numbers.clone();
        Some(BaseBehaviourEvent::LogsCollected(logs_collected))
    }
}

fn decode_ping(peer_id: PeerId, data: &[u8]) -> Option<BaseBehaviourEvent> {
    let mut ping = Ping::decode(data).map_err(|e| log::warn!("Error decoding ping: {e:?}")).ok()?;
    let worker_id = peer_id.to_base58();
    if !ping.worker_id.as_ref().is_some_and(|id| id == &worker_id) {
        log::warn!("Invalid worker_id in ping from {worker_id}");
        return None;
    }
    if !ping.verify_signature(&peer_id) {
        log::warn!("Invalid ping signature from {worker_id}");
        return None;
    }
    Some(BaseBehaviourEvent::Ping { peer_id, ping })
}

fn decode_worker_logs_msg(peer_id: PeerId, data: &[u8]) -> Option<BaseBehaviourEvent> {
    let msg = WorkerLogsMsg::decode(data)
        .map_err(|e| log::warn!("Error decoding worker logs: {e:?}"))
        .ok()?;
    match msg.msg {
        Some(worker_logs_msg::Msg::QueryLogs(query_logs)) => {
            Some(BaseBehaviourEvent::WorkerQueryLogs {
                peer_id,
                query_logs,
            })
        }
        _ => None,
    }
}

fn bundle_messages<T: prost::Message>(
    messages: impl IntoIterator<Item = T>,
    size_limit: usize,
) -> impl Iterator<Item = Vec<T>> {
    // Compute message sizes and filter out too big messages
    let mut iter = messages
        .into_iter()
        .filter_map(move |msg| {
            let msg_size = msg.encoded_len();
            if msg_size > size_limit {
                // TODO: Send oversized messages back as events, don't drop
                log::warn!("Message too big ({msg_size} > {size_limit})");
                return None;
            }
            Some((msg_size, msg))
        })
        .peekable();

    // Bundle into chunks of at most `size_limit` total size
    std::iter::from_fn(move || {
        let mut bundle = Vec::new();
        let mut remaining_cap = size_limit;
        while let Some((size, msg)) = iter.next_if(|(size, _)| size <= &remaining_cap) {
            bundle.push(msg);
            remaining_cap -= size;
        }
        (!bundle.is_empty()).then_some(bundle)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bundle_messages() {
        let messages = vec![vec![0u8; 40], vec![0u8; 40], vec![0u8; 200], vec![0u8; 90]];
        let bundles: Vec<Vec<Vec<u8>>> = bundle_messages(messages, 100).collect();

        assert_eq!(bundles.len(), 2);
        assert_eq!(bundles[0].len(), 2);
        assert_eq!(bundles[1].len(), 1);
    }
}
