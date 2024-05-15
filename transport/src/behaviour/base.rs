use std::{
    collections::{HashMap, HashSet},
    task::{Context, Poll},
    time::Duration,
    vec,
};

use bimap::BiHashMap;
use contract_client::NodeStream;
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
use prost::Message;
use serde::{Deserialize, Serialize};

use subsquid_messages::{
    broadcast_msg, signatures::SignedMessage, BroadcastMsg, LogsCollected, Ping,
};

use crate::{
    behaviour::{
        pubsub::{PubsubBehaviour, PubsubMsg},
        wrapped::{BehaviourWrapper, TToSwarm, Wrapped},
    },
    cli::BootNode,
    protocol::{ID_PROTOCOL, LOGS_TOPIC, PING_TOPIC},
    record_event,
    util::addr_is_reachable,
    PeerId, QueueFull,
};

#[cfg(feature = "metrics")]
use crate::metrics::{ACTIVE_CONNECTIONS, ONGOING_PROBES, ONGOING_QUERIES};

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseConfig {
    pub nodes_update_interval: Duration,
    pub autonat_timeout: Duration,
    pub identify_interval: Duration,
    pub request_timeout: Duration,
    pub probe_timeout: Duration,
    pub max_concurrent_probes: usize,
}

impl Default for BaseConfig {
    fn default() -> Self {
        Self {
            nodes_update_interval: Duration::from_secs(300),
            autonat_timeout: Duration::from_secs(60),
            identify_interval: Duration::from_secs(60),
            request_timeout: Duration::from_secs(60),
            probe_timeout: Duration::from_secs(60),
            max_concurrent_probes: 1000,
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
    active_nodes_stream: NodeStream,
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
        kad_config.set_replication_factor(20.try_into().unwrap());
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
            pubsub: PubsubBehaviour::new(keypair.clone()).into(),
        };

        for boot_node in boot_nodes {
            inner.allow.allow_peer(boot_node.peer_id);
            inner.kademlia.add_address(&boot_node.peer_id, boot_node.address.clone());
            inner.autonat.add_server(boot_node.peer_id, Some(boot_node.address));
        }

        Self {
            inner,
            keypair: keypair.clone(),
            ongoing_queries: Default::default(),
            outbound_conns: Default::default(),
            probe_timeouts: FuturesMap::new(config.probe_timeout, config.max_concurrent_probes),
            registered_nodes: Default::default(),
            active_nodes_stream: contract_client.network_nodes_stream(config.nodes_update_interval),
        }
    }

    pub fn subscribe_pings(&mut self) {
        self.inner.pubsub.subscribe(PING_TOPIC, false);
    }

    pub fn subscribe_logs(&mut self) {
        self.inner.pubsub.subscribe(LOGS_TOPIC, false);
    }

    pub fn sign<T: SignedMessage>(&self, msg: &mut T) {
        msg.sign(&self.keypair)
    }

    pub fn publish_ping(&mut self, mut ping: Ping) {
        self.sign(&mut ping);
        self.inner.pubsub.publish(PING_TOPIC, ping.encode_to_vec());
    }

    pub fn publish_logs_collected(&mut self, logs_collected: LogsCollected) {
        self.inner.pubsub.publish(LOGS_TOPIC, logs_collected.encode_to_vec());
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
        log::info!("Allowing peer {peer_id}");
        self.inner.allow.allow_peer(peer_id);
    }

    fn on_nodes_update(&mut self, result: Result<HashSet<PeerId>, contract_client::ClientError>) {
        let nodes = match result {
            Err(e) => {
                log::error!("Error retrieving registered nodes from chain: {e:?}");
                return;
            }
            Ok(nodes) if nodes == self.registered_nodes => {
                log::debug!("Registered nodes set unchanged.");
                return;
            }
            Ok(nodes) => nodes,
        };
        log::info!("Updating registered nodes");
        // Disallow nodes which are no longer registered
        for peer_id in self.registered_nodes.difference(&nodes) {
            log::info!("Blocking peer {peer_id}");
            self.inner.allow.disallow_peer(*peer_id);
        }
        // Allow newly registered nodes
        for peer_id in nodes.difference(&self.registered_nodes) {
            log::info!("Allowing peer {peer_id}");
            self.inner.allow.allow_peer(*peer_id);
        }
        self.registered_nodes = nodes;
    }
}

#[derive(Debug, Clone)]
pub enum BaseBehaviourEvent {
    BroadcastMsg {
        peer_id: PeerId,
        msg: BroadcastMsg,
    },
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
                    peer_id.map(|id| id.to_base58()).unwrap_or_default()
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
                    ONGOING_PROBES.dec();
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
            ONGOING_PROBES.dec();
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
            identify::Event::Received { peer_id, info } => {
                (peer_id, info.listen_addrs, info.protocols)
            }
            _ => return None,
        };
        let kademlia = &mut self.inner.kademlia;
        listen_addrs.into_iter().filter(addr_is_reachable).for_each(|addr| {
            kademlia.add_address(&peer_id, addr);
        });
        let ev = BaseBehaviourEvent::PeerProtocols { peer_id, protocols };
        Some(ToSwarm::GenerateEvent(ev))
    }

    fn on_kademlia_event(&mut self, ev: kad::Event) -> Option<TToSwarm<Self>> {
        log::debug!("Kademlia event received: {ev:?}");
        record_event(&ev);
        let (query_id, result, finished) = match ev {
            kad::Event::OutboundQueryProgressed {
                id,
                result: QueryResult::GetClosestPeers(result),
                step: ProgressStep { last, .. },
                ..
            } => (id, result, last),
            _ => return None,
        };

        let peer_id = match self.ongoing_queries.get_by_right(&query_id) {
            None => return None,
            Some(peer_id) => peer_id.to_owned(),
        };
        let peer_found = match result {
            Ok(GetClosestPeersOk { peers, .. }) => peers.contains(&peer_id),
            Err(GetClosestPeersError::Timeout { peers, .. }) => peers.contains(&peer_id),
        };

        // Query finished, or the peer has already been found. Try to dial the peer now.
        if finished || peer_found {
            log::debug!("Query for peer {peer_id} finished. peer_found={peer_found}");
            self.ongoing_queries.remove_by_right(&query_id);
            #[cfg(feature = "metrics")]
            ONGOING_QUERIES.dec();
            return Some(ToSwarm::Dial {
                opts: DialOpts::peer_id(peer_id).condition(PeerCondition::NotDialing).build(),
            });
        }
        None
    }

    fn on_autonat_event(&mut self, ev: autonat::Event) -> Option<TToSwarm<Self>> {
        log::debug!("AutoNAT event received: {ev:?}");
        let status = match ev {
            autonat::Event::StatusChanged { new, .. } => new,
            _ => return None,
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
        log::debug!("Pub-sub message received: peer_id={peer_id} topic={topic}");
        let msg = match topic {
            PING_TOPIC => decode_ping(&peer_id, data)?,
            LOGS_TOPIC => decode_logs_collected(data)?,
            _ => return None,
        };
        let ev = BaseBehaviourEvent::BroadcastMsg { peer_id, msg };
        Some(ToSwarm::GenerateEvent(ev))
    }
}

fn decode_ping(peer_id: &PeerId, data: Box<[u8]>) -> Option<BroadcastMsg> {
    let mut ping = Ping::decode(data.as_ref())
        .map_err(|e| log::warn!("Error decoding ping: {e:?}"))
        .ok()?;
    if !ping.verify_signature(peer_id) {
        log::warn!("Invalid ping signature from {peer_id}");
        return None;
    }
    Some(BroadcastMsg {
        msg: Some(broadcast_msg::Msg::Ping(ping)),
    })
}

fn decode_logs_collected(data: Box<[u8]>) -> Option<BroadcastMsg> {
    let logs_collected = LogsCollected::decode(data.as_ref())
        .map_err(|e| log::warn!("Error decoding logs collected: {e:?}"))
        .ok()?;
    Some(BroadcastMsg {
        msg: Some(broadcast_msg::Msg::LogsCollected(logs_collected)),
    })
}
