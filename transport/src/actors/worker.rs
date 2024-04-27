use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
    time::Duration,
};

use futures::StreamExt;
use futures_core::Stream;
use libp2p::{
    request_response::ResponseChannel,
    swarm::{NetworkBehaviour, SwarmEvent, ToSwarm},
    PeerId, Swarm,
};
use libp2p_swarm_derive::NetworkBehaviour;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use subsquid_messages::{
    broadcast_msg, envelope, signatures::SignedMessage, BroadcastMsg, Envelope, LogsCollected,
    Ping, Pong, Query, QueryExecuted, QueryLogs, QueryResult,
};

use crate::{
    behaviour::{
        base::{BaseBehaviour, BaseBehaviourEvent, ACK_SIZE},
        request_client::{ClientBehaviour, ClientConfig, ClientEvent},
        request_server::{Request, ServerBehaviour},
        wrapped::{BehaviourWrapper, TToSwarm, Wrapped},
    },
    codec::ProtoCodec,
    util::TaskManager,
    QueueFull,
};

use crate::protocol::{
    MAX_PONG_SIZE, MAX_QUERY_RESULT_SIZE, MAX_QUERY_SIZE, MAX_WORKER_LOGS_SIZE, PONG_PROTOCOL,
    QUERY_PROTOCOL, WORKER_LOGS_PROTOCOL,
};
#[cfg(feature = "metrics")]
use libp2p::metrics::{Metrics, Recorder};
#[cfg(feature = "metrics")]
use prometheus_client::registry::Registry;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerEvent {
    /// Pong message received from the scheduler
    Pong(Pong),
    /// Query received from a gateway
    Query(Query),
    /// Logs up to `last_seq_no` have been saved by logs collector
    LogsCollected { last_seq_no: u64 },
}

type PongBehaviour = Wrapped<ServerBehaviour<ProtoCodec<Pong, u32>>>;
type QueryBehaviour = Wrapped<ServerBehaviour<ProtoCodec<Query, QueryResult>>>;
type LogsBehaviour = Wrapped<ClientBehaviour<ProtoCodec<QueryLogs, u32>>>;

#[derive(NetworkBehaviour)]
pub struct InnerBehaviour {
    base: Wrapped<BaseBehaviour>,
    pong: PongBehaviour,
    query: QueryBehaviour,
    logs: LogsBehaviour,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub local_peer_id: PeerId,
    pub scheduler_id: PeerId,
    pub logs_collector_id: PeerId,
    pub max_pong_size: u64,
    pub max_query_size: u64,
    pub max_query_result_size: u64,
    pub max_query_logs_size: u64,
    pub logs_config: ClientConfig,
    pub pings_queue_size: usize,
    pub query_results_queue_size: usize,
    pub logs_queue_size: usize,
    pub events_queue_size: usize,
    pub shutdown_timeout: Duration,
}

impl WorkerConfig {
    pub fn new(local_peer_id: PeerId, scheduler_id: PeerId, logs_collector_id: PeerId) -> Self {
        Self {
            local_peer_id,
            scheduler_id,
            logs_collector_id,
            max_pong_size: MAX_PONG_SIZE,
            max_query_size: MAX_QUERY_SIZE,
            max_query_result_size: MAX_QUERY_RESULT_SIZE,
            max_query_logs_size: MAX_WORKER_LOGS_SIZE,
            logs_config: Default::default(),
            pings_queue_size: 100,
            query_results_queue_size: 100,
            logs_queue_size: 100,
            events_queue_size: 100,
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}

pub struct WorkerBehaviour {
    inner: InnerBehaviour,
    local_peer_id: String,
    scheduler_id: PeerId,
    logs_collector_id: PeerId,
    query_senders: HashMap<String, PeerId>,
    query_response_channels: HashMap<String, ResponseChannel<QueryResult>>,
}

impl WorkerBehaviour {
    pub fn new(mut base: BaseBehaviour, config: WorkerConfig) -> Wrapped<Self> {
        base.subscribe_pings();
        base.subscribe_logs_collected();
        Self {
            inner: InnerBehaviour {
                base: base.into(),
                pong: ServerBehaviour::new(
                    ProtoCodec::new(config.max_pong_size, ACK_SIZE),
                    PONG_PROTOCOL,
                )
                .into(),
                query: ServerBehaviour::new(
                    ProtoCodec::new(config.max_query_size, config.max_query_result_size),
                    QUERY_PROTOCOL,
                )
                .into(),
                logs: ClientBehaviour::new(
                    ProtoCodec::new(config.max_query_logs_size, ACK_SIZE),
                    WORKER_LOGS_PROTOCOL,
                    config.logs_config,
                )
                .into(),
            },
            local_peer_id: config.local_peer_id.to_string(),
            scheduler_id: config.scheduler_id,
            logs_collector_id: config.logs_collector_id,
            query_senders: Default::default(),
            query_response_channels: Default::default(),
        }
        .into()
    }
    #[rustfmt::skip]
    fn on_base_event(&mut self, ev: BaseBehaviourEvent) -> Option<WorkerEvent> {
        match ev {
            BaseBehaviourEvent::BroadcastMsg {
                peer_id,
                msg: BroadcastMsg{ msg: Some(broadcast_msg::Msg::LogsCollected(msg)) },
            } => self.on_logs_collected(peer_id, msg),
            BaseBehaviourEvent::LegacyMsg {
                peer_id,
                envelope: Envelope{ msg: Some(envelope::Msg::Query(query)) },
            } => self.on_query(peer_id, query, None),
            _ => None
        }
    }

    fn on_logs_collected(
        &mut self,
        peer_id: PeerId,
        mut logs_collected: LogsCollected,
    ) -> Option<WorkerEvent> {
        if peer_id != self.logs_collector_id {
            log::warn!("Peer {peer_id} impersonating logs collector");
            self.inner.base.block_peer(peer_id);
            return None;
        }
        log::debug!("Received logs collected message");
        // Extract last_seq_no for the local worker
        logs_collected
            .sequence_numbers
            .remove(&self.local_peer_id)
            .map(|last_seq_no| WorkerEvent::LogsCollected { last_seq_no })
    }

    fn on_query(
        &mut self,
        peer_id: PeerId,
        mut query: Query,
        resp_chan: Option<ResponseChannel<QueryResult>>,
    ) -> Option<WorkerEvent> {
        // Verify query signature
        if !query.verify_signature(&peer_id) {
            log::warn!("Dropping query with invalid signature from {peer_id}");
            return None;
        }
        // Check if query has ID
        let query_id = match &query.query_id {
            Some(id) => id.clone(),
            None => {
                log::warn!("Dropping query without ID from {peer_id}");
                return None;
            }
        };
        // Check if query ID is not duplicated
        match self.query_senders.entry(query_id.clone()) {
            Entry::Occupied(e) => {
                log::warn!("Duplicate query ID: {}", e.key());
                return None;
            }
            Entry::Vacant(e) => {
                e.insert(peer_id);
            }
        }
        log::debug!("Query {query_id} verified");
        if let Some(resp_chan) = resp_chan {
            self.query_response_channels.insert(query_id, resp_chan);
        }
        Some(WorkerEvent::Query(query))
    }

    fn on_pong_event(
        &mut self,
        Request {
            peer_id,
            request,
            response_channel,
        }: Request<Pong, u32>,
    ) -> Option<WorkerEvent> {
        if peer_id != self.scheduler_id {
            log::warn!("Peer {peer_id} impersonating scheduler");
            self.inner.base.block_peer(peer_id);
            return None;
        }
        log::debug!("Received pong from scheduler: {request:?}");
        // Send minimal response to avoid getting errors
        _ = self.inner.pong.try_send_response(response_channel, 1);
        Some(WorkerEvent::Pong(request))
    }

    fn on_logs_event(&mut self, ev: ClientEvent<u32>) -> Option<WorkerEvent> {
        match ev {
            ClientEvent::Response { .. } => {} // response is just ACK, no useful information
            ClientEvent::PeerUnknown { peer_id } => self.inner.base.find_and_dial(peer_id),
            ClientEvent::Timeout { .. } => log::error!("Sending logs failed"),
        }
        None
    }

    pub fn send_ping(&mut self, ping: Ping) {
        self.inner.base.publish_ping(ping);
    }

    pub fn send_query_result(&mut self, result: QueryResult) {
        log::debug!("Sending query result {result:?}");
        let sender_id = match self.query_senders.remove(&result.query_id) {
            Some(peer_id) => peer_id,
            None => return log::error!("Unknown query: {}", result.query_id),
        };
        let resp_chan = match self.query_response_channels.remove(&result.query_id) {
            Some(ch) => ch,
            None => return self.inner.base.send_legacy_msg(&sender_id, result), // Handle queries from legacy clients
        };
        self.inner
            .query
            .try_send_response(resp_chan, result)
            .unwrap_or_else(|e| log::error!("Cannot send result for query {}", e.query_id));
    }

    pub fn send_logs(&mut self, logs: Vec<QueryExecuted>) {
        log::debug!("Sending query logs");
        // TODO: Bundle logs
        let logs = QueryLogs {
            queries_executed: logs,
        };
        let peer_id = self.logs_collector_id;
        if self.inner.logs.try_send_request(peer_id, logs).is_err() {
            log::error!("Cannot send query logs: outbound queue full")
        }
    }
}

impl BehaviourWrapper for WorkerBehaviour {
    type Inner = InnerBehaviour;
    type Event = WorkerEvent;

    fn inner(&mut self) -> &mut Self::Inner {
        &mut self.inner
    }

    fn on_inner_event(
        &mut self,
        ev: <Self::Inner as NetworkBehaviour>::ToSwarm,
    ) -> impl IntoIterator<Item = TToSwarm<Self>> {
        let ev = match ev {
            InnerBehaviourEvent::Base(ev) => self.on_base_event(ev),
            InnerBehaviourEvent::Pong(ev) => self.on_pong_event(ev),
            InnerBehaviourEvent::Query(Request {
                peer_id,
                request,
                response_channel,
            }) => self.on_query(peer_id, request, Some(response_channel)),
            InnerBehaviourEvent::Logs(ev) => self.on_logs_event(ev),
        };
        ev.map(ToSwarm::GenerateEvent)
    }
}

struct WorkerTransport {
    swarm: Swarm<Wrapped<WorkerBehaviour>>,
    pings_rx: mpsc::Receiver<Ping>,
    query_results_rx: mpsc::Receiver<QueryResult>,
    logs_rx: mpsc::Receiver<Vec<QueryExecuted>>,
    events_tx: mpsc::Sender<WorkerEvent>,
    #[cfg(feature = "metrics")]
    metrics: Metrics,
}

impl WorkerTransport {
    pub async fn run(mut self, cancel_token: CancellationToken) {
        log::info!("Starting worker P2P transport");
        loop {
            tokio::select! {
                 _ = cancel_token.cancelled() => break,
                ev = self.swarm.select_next_some() => self.on_swarm_event(ev),
                Some(ping) = self.pings_rx.recv() => self.swarm.behaviour_mut().send_ping(ping),
                Some(res) = self.query_results_rx.recv() => self.swarm.behaviour_mut().send_query_result(res),
                Some(logs) = self.logs_rx.recv() => self.swarm.behaviour_mut().send_logs(logs),
            }
        }
        log::info!("Shutting down worker P2P transport");
    }

    fn on_swarm_event(&mut self, ev: SwarmEvent<WorkerEvent>) {
        #[cfg(feature = "metrics")]
        self.metrics.record(&ev);
        if let SwarmEvent::Behaviour(ev) = ev {
            self.events_tx
                .try_send(ev)
                .unwrap_or_else(|e| log::error!("Worker event queue full. Event dropped: {e:?}"))
        }
    }
}

#[derive(Clone)]
pub struct WorkerTransportHandle {
    pings_tx: mpsc::Sender<Ping>,
    query_results_tx: mpsc::Sender<QueryResult>,
    logs_tx: mpsc::Sender<Vec<QueryExecuted>>,
    _task_manager: Arc<TaskManager>, // This ensures that transport is stopped when the last handle is dropped
}

impl WorkerTransportHandle {
    fn new(
        pings_tx: mpsc::Sender<Ping>,
        query_results_tx: mpsc::Sender<QueryResult>,
        logs_tx: mpsc::Sender<Vec<QueryExecuted>>,
        transport: WorkerTransport,
        shutdown_timeout: Duration,
    ) -> Self {
        let mut task_manager = TaskManager::new(shutdown_timeout);
        task_manager.spawn(|c| transport.run(c));
        Self {
            pings_tx,
            query_results_tx,
            logs_tx,
            _task_manager: Arc::new(task_manager),
        }
    }

    pub fn send_ping(&self, ping: Ping) -> Result<(), QueueFull> {
        log::debug!("Queueing ping {ping:?}");
        Ok(self.pings_tx.try_send(ping)?)
    }

    pub fn send_query_result(&self, result: QueryResult) -> Result<(), QueueFull> {
        log::debug!("Queueing query result {result:?}");
        Ok(self.query_results_tx.try_send(result)?)
    }

    pub fn send_logs(&self, logs: Vec<QueryExecuted>) -> Result<(), QueueFull> {
        log::debug!("Queueing {} query logs", logs.len());
        Ok(self.logs_tx.try_send(logs)?)
    }
}

pub fn start_transport(
    swarm: Swarm<Wrapped<WorkerBehaviour>>,
    config: WorkerConfig,
    #[cfg(feature = "metrics")] registry: &mut Registry,
) -> (impl Stream<Item = WorkerEvent>, WorkerTransportHandle) {
    let (pings_tx, pings_rx) = mpsc::channel(config.pings_queue_size);
    let (query_results_tx, query_results_rx) = mpsc::channel(config.query_results_queue_size);
    let (logs_tx, logs_rx) = mpsc::channel(config.logs_queue_size);
    let (events_tx, events_rx) = mpsc::channel(config.events_queue_size);
    let transport = WorkerTransport {
        swarm,
        pings_rx,
        query_results_rx,
        logs_rx,
        events_tx,
        #[cfg(feature = "metrics")]
        metrics: Metrics::new(registry),
    };
    let handle = WorkerTransportHandle::new(
        pings_tx,
        query_results_tx,
        logs_tx,
        transport,
        config.shutdown_timeout,
    );
    (ReceiverStream::new(events_rx), handle)
}