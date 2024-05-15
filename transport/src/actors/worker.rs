use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::StreamExt;
use futures_core::Stream;
use libp2p::{
    request_response::ResponseChannel,
    swarm::{NetworkBehaviour, SwarmEvent, ToSwarm},
    PeerId, Swarm,
};
use libp2p_swarm_derive::NetworkBehaviour;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use subsquid_messages::{
    broadcast_msg, signatures::SignedMessage, BroadcastMsg, LogsCollected, Ping, Pong, Query,
    QueryExecuted, QueryLogs, QueryResult,
};

use crate::{
    behaviour::{
        base::{BaseBehaviour, BaseBehaviourEvent},
        request_client::{ClientBehaviour, ClientConfig, ClientEvent},
        request_server::{Request, ServerBehaviour},
        wrapped::{BehaviourWrapper, TToSwarm, Wrapped},
    },
    codec::{ProtoCodec, ACK_SIZE},
    protocol::{
        MAX_PONG_SIZE, MAX_QUERY_RESULT_SIZE, MAX_QUERY_SIZE, MAX_WORKER_LOGS_SIZE, PONG_PROTOCOL,
        QUERY_PROTOCOL, WORKER_LOGS_PROTOCOL,
    },
    record_event,
    util::{new_queue, Receiver, Sender, TaskManager, DEFAULT_SHUTDOWN_TIMEOUT},
    QueueFull,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerEvent {
    /// Pong message received from the scheduler
    Pong(Pong),
    /// Query received from a gateway
    Query { peer_id: PeerId, query: Query },
    /// Logs up to `last_seq_no` have been saved by logs collector
    LogsCollected { last_seq_no: Option<u64> },
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
    pub fn new(scheduler_id: PeerId, logs_collector_id: PeerId) -> Self {
        Self {
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
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

pub struct WorkerBehaviour {
    inner: InnerBehaviour,
    local_peer_id: String,
    scheduler_id: PeerId,
    logs_collector_id: PeerId,
    max_query_logs_size: usize,
    query_response_channels: HashMap<String, ResponseChannel<QueryResult>>,
}

impl WorkerBehaviour {
    pub fn new(
        mut base: BaseBehaviour,
        local_peer_id: PeerId,
        config: WorkerConfig,
    ) -> Wrapped<Self> {
        base.subscribe_pings();
        base.subscribe_logs();
        base.allow_peer(config.logs_collector_id);
        base.allow_peer(config.scheduler_id);
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
            local_peer_id: local_peer_id.to_base58(),
            scheduler_id: config.scheduler_id,
            logs_collector_id: config.logs_collector_id,
            max_query_logs_size: config.max_query_logs_size as usize,
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
            return None;
        }
        log::debug!("Received logs collected message");
        // Extract last_seq_no for the local worker
        let last_seq_no = logs_collected.sequence_numbers.remove(&self.local_peer_id);
        Some(WorkerEvent::LogsCollected { last_seq_no })
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
        log::debug!("Query {query_id} verified");
        if let Some(resp_chan) = resp_chan {
            self.query_response_channels.insert(query_id, resp_chan);
        }
        Some(WorkerEvent::Query { peer_id, query })
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
        let resp_chan = match self.query_response_channels.remove(&result.query_id) {
            Some(ch) => ch,
            None => return log::error!("No response channel for query: {}", result.query_id),
        };
        self.inner
            .query
            .try_send_response(resp_chan, result)
            .unwrap_or_else(|e| log::error!("Cannot send result for query {}", e.query_id));
    }

    pub fn send_logs(&mut self, mut logs: Vec<QueryExecuted>) {
        log::debug!("Sending query logs");
        for log in logs.iter_mut() {
            self.inner.base.sign(log);
        }
        let peer_id = self.logs_collector_id;
        for bundle in bundle_messages(logs, self.max_query_logs_size) {
            let logs = QueryLogs {
                queries_executed: bundle,
            };
            if self.inner.logs.try_send_request(peer_id, logs).is_err() {
                log::error!("Cannot send query logs: outbound queue full")
            }
        }
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
    pings_rx: Receiver<Ping>,
    query_results_rx: Receiver<QueryResult>,
    logs_rx: Receiver<Vec<QueryExecuted>>,
    events_tx: Sender<WorkerEvent>,
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
        log::trace!("Swarm event: {ev:?}");
        record_event(&ev);
        if let SwarmEvent::Behaviour(ev) = ev {
            self.events_tx.send_lossy(ev)
        }
    }
}

#[derive(Clone)]
pub struct WorkerTransportHandle {
    pings_tx: Sender<Ping>,
    query_results_tx: Sender<QueryResult>,
    logs_tx: Sender<Vec<QueryExecuted>>,
    _task_manager: Arc<TaskManager>, // This ensures that transport is stopped when the last handle is dropped
}

impl WorkerTransportHandle {
    fn new(
        pings_tx: Sender<Ping>,
        query_results_tx: Sender<QueryResult>,
        logs_tx: Sender<Vec<QueryExecuted>>,
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
        self.pings_tx.try_send(ping)
    }

    pub fn send_query_result(&self, result: QueryResult) -> Result<(), QueueFull> {
        log::debug!("Queueing query result {result:?}");
        self.query_results_tx.try_send(result)
    }

    pub fn send_logs(&self, logs: Vec<QueryExecuted>) -> Result<(), QueueFull> {
        log::debug!("Queueing {} query logs", logs.len());
        self.logs_tx.try_send(logs)
    }
}

pub fn start_transport(
    swarm: Swarm<Wrapped<WorkerBehaviour>>,
    config: WorkerConfig,
) -> (impl Stream<Item = WorkerEvent>, WorkerTransportHandle) {
    let (pings_tx, pings_rx) = new_queue(config.pings_queue_size, "pings");
    let (query_results_tx, query_results_rx) =
        new_queue(config.query_results_queue_size, "query_results");
    let (logs_tx, logs_rx) = new_queue(config.logs_queue_size, "logs");
    let (events_tx, events_rx) = new_queue(config.events_queue_size, "events");
    let transport = WorkerTransport {
        swarm,
        pings_rx,
        query_results_rx,
        logs_rx,
        events_tx,
    };
    let handle = WorkerTransportHandle::new(
        pings_tx,
        query_results_tx,
        logs_tx,
        transport,
        config.shutdown_timeout,
    );
    (events_rx, handle)
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
