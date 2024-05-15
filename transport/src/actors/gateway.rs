use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures::StreamExt;
use futures_core::Stream;
use libp2p::{
    request_response::OutboundRequestId,
    swarm::{NetworkBehaviour, SwarmEvent, ToSwarm},
    PeerId, Swarm,
};
use libp2p_swarm_derive::NetworkBehaviour;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use subsquid_messages::{
    broadcast_msg, gateway_log_msg, query_result, BroadcastMsg, GatewayLogMsg, Ping, Query,
    QueryFinished, QueryResult, QuerySubmitted,
};

use crate::{
    behaviour::{
        base::{BaseBehaviour, BaseBehaviourEvent},
        request_client::{ClientBehaviour, ClientConfig, ClientEvent},
        wrapped::{BehaviourWrapper, TToSwarm, Wrapped},
    },
    codec::{ProtoCodec, ACK_SIZE},
    protocol::{
        GATEWAY_LOGS_PROTOCOL, MAX_GATEWAY_LOG_SIZE, MAX_QUERY_RESULT_SIZE, MAX_QUERY_SIZE,
        QUERY_PROTOCOL,
    },
    record_event,
    util::{new_queue, Receiver, Sender, TaskManager, DEFAULT_SHUTDOWN_TIMEOUT},
    QueueFull,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GatewayEvent {
    Ping {
        peer_id: PeerId,
        ping: Ping,
    },
    QueryResult {
        peer_id: PeerId,
        result: QueryResult,
    },
}

#[derive(NetworkBehaviour)]
pub struct InnerBehaviour {
    base: Wrapped<BaseBehaviour>,
    query: Wrapped<ClientBehaviour<ProtoCodec<Query, QueryResult>>>,
    logs: Wrapped<ClientBehaviour<ProtoCodec<GatewayLogMsg, u32>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    pub logs_collector_id: PeerId,
    pub query_config: ClientConfig,
    pub logs_config: ClientConfig,
    pub max_query_size: u64,
    pub max_query_result_size: u64,
    pub max_query_log_size: u64,
    pub queries_queue_size: usize,
    pub logs_queue_size: usize,
    pub events_queue_size: usize,
    pub shutdown_timeout: Duration,
}

impl GatewayConfig {
    pub fn new(logs_collector_id: PeerId) -> Self {
        Self {
            logs_collector_id,
            query_config: Default::default(),
            logs_config: Default::default(),
            max_query_size: MAX_QUERY_SIZE,
            max_query_result_size: MAX_QUERY_RESULT_SIZE,
            max_query_log_size: MAX_GATEWAY_LOG_SIZE,
            queries_queue_size: 100,
            logs_queue_size: 100,
            events_queue_size: 100,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

pub struct GatewayBehaviour {
    inner: InnerBehaviour,
    logs_collector_id: PeerId,
    query_ids: BTreeMap<OutboundRequestId, String>,
}

impl GatewayBehaviour {
    pub fn new(mut base: BaseBehaviour, config: GatewayConfig) -> Wrapped<Self> {
        base.subscribe_pings();
        base.allow_peer(config.logs_collector_id);
        let inner = InnerBehaviour {
            base: base.into(),
            query: ClientBehaviour::new(
                ProtoCodec::new(config.max_query_size, config.max_query_result_size),
                QUERY_PROTOCOL,
                config.query_config,
            )
            .into(),
            logs: ClientBehaviour::new(
                ProtoCodec::new(config.max_query_log_size, ACK_SIZE),
                GATEWAY_LOGS_PROTOCOL,
                config.logs_config,
            )
            .into(),
        };
        Self {
            inner,
            logs_collector_id: config.logs_collector_id,
            query_ids: Default::default(),
        }
        .into()
    }
    fn on_base_event(&mut self, ev: BaseBehaviourEvent) -> Option<GatewayEvent> {
        match ev {
            BaseBehaviourEvent::BroadcastMsg {
                peer_id,
                msg:
                    BroadcastMsg {
                        msg: Some(broadcast_msg::Msg::Ping(ping)),
                    },
            } => self.on_ping(peer_id, ping),
            _ => None,
        }
    }

    fn on_ping(&mut self, peer_id: PeerId, ping: Ping) -> Option<GatewayEvent> {
        log::debug!("Got ping from {peer_id}: {ping:?}");
        match ping.worker_id.as_ref() {
            Some(id) if id.parse().is_ok_and(|id: PeerId| id == peer_id) => {}
            _ => {
                log::error!("Rejecting ping with invalid worker ID");
                return None;
            }
        }
        Some(GatewayEvent::Ping { peer_id, ping })
    }

    fn on_query_result(
        &mut self,
        peer_id: PeerId,
        result: QueryResult,
        req_id: Option<OutboundRequestId>,
    ) -> Option<GatewayEvent> {
        log::debug!("Got query result from {peer_id}: {result:?}");
        // Verify if query ID matches request ID
        if let Some(req_id) = req_id {
            match self.query_ids.remove(&req_id) {
                Some(query_id) if query_id == result.query_id => {}
                _ => {
                    log::error!("Unknown request ID: {req_id}");
                    return None;
                }
            }
        }
        Some(GatewayEvent::QueryResult { peer_id, result })
    }

    fn on_query_event(&mut self, ev: ClientEvent<QueryResult>) -> Option<GatewayEvent> {
        match ev {
            ClientEvent::Response {
                peer_id,
                req_id,
                response,
            } => self.on_query_result(peer_id, response, Some(req_id)),
            ClientEvent::Timeout { req_id, peer_id } => self.on_query_timeout(req_id, peer_id),
            ClientEvent::PeerUnknown { peer_id } => {
                self.inner.base.find_and_dial(peer_id);
                None
            }
        }
    }

    fn on_query_timeout(
        &mut self,
        req_id: OutboundRequestId,
        peer_id: PeerId,
    ) -> Option<GatewayEvent> {
        let query_id = match self.query_ids.remove(&req_id) {
            Some(id) => id,
            None => {
                log::error!("Unknown request ID: {req_id}");
                return None;
            }
        };
        log::debug!("Query {query_id} timed out");
        Some(GatewayEvent::QueryResult {
            peer_id,
            result: QueryResult {
                query_id,
                result: Some(query_result::Result::Timeout(())),
            },
        })
    }

    fn on_logs_event(&mut self, ev: ClientEvent<u32>) -> Option<GatewayEvent> {
        log::debug!("Logs event: {ev:?}");
        match ev {
            ClientEvent::PeerUnknown { peer_id } => self.inner.base.find_and_dial(peer_id),
            ClientEvent::Timeout { .. } => log::warn!("Sending logs to collector timed out"),
            _ => {}
        }
        None
    }

    pub fn send_query(&mut self, peer_id: PeerId, mut query: Query) {
        log::debug!("Sending query {query:?} to {peer_id}");
        // Validate if query has ID and sign it
        let query_id = match query.query_id.as_ref() {
            Some(id) => id.clone(),
            None => return log::error!("Query without ID dropped"),
        };
        self.inner.base.sign(&mut query);
        match self.inner.query.try_send_request(peer_id, query) {
            Ok(req_id) => {
                self.query_ids.insert(req_id, query_id);
            }
            Err(_) => log::error!("Outbound message queue full. Query {query_id} dropped."),
        }
    }

    pub fn send_log_msg(&mut self, msg: GatewayLogMsg) {
        log::debug!("Sending log message: {msg:?}");
        if self.inner.logs.try_send_request(self.logs_collector_id, msg).is_err() {
            log::error!("Cannot send query logs: outbound queue full")
        }
    }
}

impl BehaviourWrapper for GatewayBehaviour {
    type Inner = InnerBehaviour;
    type Event = GatewayEvent;

    fn inner(&mut self) -> &mut Self::Inner {
        &mut self.inner
    }

    fn on_inner_event(
        &mut self,
        ev: <Self::Inner as NetworkBehaviour>::ToSwarm,
    ) -> impl IntoIterator<Item = TToSwarm<Self>> {
        let ev = match ev {
            InnerBehaviourEvent::Base(ev) => self.on_base_event(ev),
            InnerBehaviourEvent::Query(query_res) => self.on_query_event(query_res),
            InnerBehaviourEvent::Logs(ev) => self.on_logs_event(ev),
        };
        ev.map(ToSwarm::GenerateEvent)
    }
}

struct GatewayTransport {
    swarm: Swarm<Wrapped<GatewayBehaviour>>,
    queries_rx: Receiver<(PeerId, Query)>,
    logs_rx: Receiver<GatewayLogMsg>,
    events_tx: Sender<GatewayEvent>,
}

impl GatewayTransport {
    pub async fn run(mut self, cancel_token: CancellationToken) {
        log::info!("Starting gateway P2P transport");
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                ev = self.swarm.select_next_some() => self.on_swarm_event(ev),
                Some((peer_id, query)) = self.queries_rx.recv() => self.swarm.behaviour_mut().send_query(peer_id, query),
                Some(log_msg) = self.logs_rx.recv() => self.swarm.behaviour_mut().send_log_msg(log_msg),
            }
        }
        log::info!("Shutting down gateway P2P transport");
    }

    fn on_swarm_event(&mut self, ev: SwarmEvent<GatewayEvent>) {
        log::trace!("Swarm event: {ev:?}");
        record_event(&ev);
        if let SwarmEvent::Behaviour(ev) = ev {
            self.events_tx.send_lossy(ev)
        }
    }
}

#[derive(Clone)]
pub struct GatewayTransportHandle {
    queries_tx: Sender<(PeerId, Query)>,
    logs_tx: Sender<GatewayLogMsg>,
    _task_manager: Arc<TaskManager>,
}

impl GatewayTransportHandle {
    fn new(
        queries_tx: Sender<(PeerId, Query)>,
        logs_tx: Sender<GatewayLogMsg>,
        transport: GatewayTransport,
        shutdown_timeout: Duration,
    ) -> Self {
        let mut task_manager = TaskManager::new(shutdown_timeout);
        task_manager.spawn(|c| transport.run(c));
        Self {
            queries_tx,
            logs_tx,
            _task_manager: Arc::new(task_manager),
        }
    }
    pub fn send_query(&self, peer_id: PeerId, query: Query) -> Result<(), QueueFull> {
        log::debug!("Queueing query {query:?}");
        self.queries_tx.try_send((peer_id, query))
    }

    pub fn query_submitted(&self, msg: QuerySubmitted) -> Result<(), QueueFull> {
        log::debug!("Queueing QuerySubmitted message: {msg:?}");
        let msg = GatewayLogMsg {
            msg: Some(gateway_log_msg::Msg::QuerySubmitted(msg)),
        };
        self.logs_tx.try_send(msg)
    }

    pub fn query_finished(&self, msg: QueryFinished) -> Result<(), QueueFull> {
        log::debug!("Queueing QueryFinished message: {msg:?}");
        let msg = GatewayLogMsg {
            msg: Some(gateway_log_msg::Msg::QueryFinished(msg)),
        };
        self.logs_tx.try_send(msg)
    }
}

pub fn start_transport(
    swarm: Swarm<Wrapped<GatewayBehaviour>>,
    config: GatewayConfig,
) -> (impl Stream<Item = GatewayEvent>, GatewayTransportHandle) {
    let (queries_tx, queries_rx) = new_queue(config.queries_queue_size, "queries");
    let (logs_tx, logs_rx) = new_queue(config.logs_queue_size, "logs");
    let (events_tx, events_rx) = new_queue(config.events_queue_size, "events");
    let transport = GatewayTransport {
        swarm,
        queries_rx,
        logs_rx,
        events_tx,
    };
    let handle =
        GatewayTransportHandle::new(queries_tx, logs_tx, transport, config.shutdown_timeout);
    (events_rx, handle)
}
