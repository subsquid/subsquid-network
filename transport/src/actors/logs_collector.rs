use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use futures_core::Stream;
use libp2p::{
    swarm::{NetworkBehaviour, SwarmEvent, ToSwarm},
    PeerId, Swarm,
};
use libp2p_swarm_derive::NetworkBehaviour;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use subsquid_messages::{
    gateway_log_msg, signatures::SignedMessage, GatewayLogMsg, LogsCollected, QueryExecuted,
    QueryFinished, QueryLogs, QuerySubmitted,
};

use crate::{
    behaviour::{
        base::{BaseBehaviour, BaseBehaviourEvent},
        request_server::{Request, ServerBehaviour},
        wrapped::{BehaviourWrapper, TToSwarm, Wrapped},
    },
    codec::{ProtoCodec, ACK_SIZE},
    protocol::{GATEWAY_LOGS_PROTOCOL, MAX_GATEWAY_LOG_SIZE},
    record_event,
    util::{new_queue, Receiver, Sender, TaskManager, DEFAULT_SHUTDOWN_TIMEOUT},
    QueueFull,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogsCollectorEvent {
    /// Worker reports executed queries (a bundle)
    WorkerLogs {
        peer_id: PeerId,
        logs: Vec<QueryExecuted>,
    },
    /// Gateway reports a submitted query
    QuerySubmitted(QuerySubmitted),
    /// Gateway reports a finished query (result received or timeout)
    QueryFinished(QueryFinished),
}

#[derive(NetworkBehaviour)]
pub struct InnerBehaviour {
    base: Wrapped<BaseBehaviour>,
    gateway_logs: Wrapped<ServerBehaviour<ProtoCodec<GatewayLogMsg, u32>>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LogsCollectorConfig {
    pub max_gateway_log_size: u64,
    pub logs_collected_queue_size: usize,
    pub events_queue_size: usize,
    pub shutdown_timeout: Duration,
}

impl Default for LogsCollectorConfig {
    fn default() -> Self {
        Self {
            max_gateway_log_size: MAX_GATEWAY_LOG_SIZE,
            logs_collected_queue_size: 100,
            events_queue_size: 100,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

pub struct LogsCollectorBehaviour {
    inner: InnerBehaviour,
}

impl LogsCollectorBehaviour {
    pub fn new(mut base: BaseBehaviour, config: LogsCollectorConfig) -> Wrapped<Self> {
        base.subscribe_worker_logs();
        base.subscribe_logs_collected();
        Self {
            inner: InnerBehaviour {
                base: base.into(),
                gateway_logs: ServerBehaviour::new(
                    ProtoCodec::new(config.max_gateway_log_size, ACK_SIZE),
                    GATEWAY_LOGS_PROTOCOL,
                )
                .into(),
            },
        }
        .into()
    }

    fn on_worker_logs(&mut self, peer_id: PeerId, logs: QueryLogs) -> Option<LogsCollectorEvent> {
        let mut logs = logs.queries_executed;
        log::debug!("Got {} query logs from {peer_id}", logs.len());
        let worker_id = peer_id.to_base58();
        logs = logs
            .into_iter()
            .filter_map(|mut log| {
                (log.worker_id == worker_id && log.verify_signature(&peer_id)).then_some(log)
            })
            .collect();
        (!logs.is_empty()).then_some(LogsCollectorEvent::WorkerLogs { peer_id, logs })
    }

    fn on_gateway_log(
        &mut self,
        peer_id: PeerId,
        log_msg: GatewayLogMsg,
    ) -> Option<LogsCollectorEvent> {
        log::debug!("Got gateway log from {peer_id}: {log_msg:?}");
        let gateway_id = peer_id.to_base58();
        match log_msg.msg {
            Some(gateway_log_msg::Msg::QueryFinished(log)) if log.client_id == gateway_id => {
                Some(LogsCollectorEvent::QueryFinished(log))
            }
            Some(gateway_log_msg::Msg::QuerySubmitted(log)) if log.client_id == gateway_id => {
                Some(LogsCollectorEvent::QuerySubmitted(log))
            }
            _ => {
                log::warn!("Invalid gateway log message: {log_msg:?}");
                None
            }
        }
    }

    pub fn logs_collected(&mut self, logs_collected: LogsCollected) {
        self.inner.base.publish_logs_collected(logs_collected)
    }
}

impl BehaviourWrapper for LogsCollectorBehaviour {
    type Inner = InnerBehaviour;
    type Event = LogsCollectorEvent;

    fn inner(&mut self) -> &mut Self::Inner {
        &mut self.inner
    }

    fn on_inner_event(
        &mut self,
        ev: <Self::Inner as NetworkBehaviour>::ToSwarm,
    ) -> impl IntoIterator<Item = TToSwarm<Self>> {
        let ev = match ev {
            InnerBehaviourEvent::Base(BaseBehaviourEvent::WorkerQueryLogs {
                peer_id,
                query_logs,
            }) => self.on_worker_logs(peer_id, query_logs),
            InnerBehaviourEvent::GatewayLogs(Request {
                peer_id,
                request,
                response_channel,
            }) => {
                _ = self.inner.gateway_logs.try_send_response(response_channel, 1);
                self.on_gateway_log(peer_id, request)
            }
            _ => None,
        };
        ev.map(ToSwarm::GenerateEvent)
    }
}

struct LogsCollectorTransport {
    swarm: Swarm<Wrapped<LogsCollectorBehaviour>>,
    logs_collected_rx: Receiver<LogsCollected>,
    events_tx: Sender<LogsCollectorEvent>,
}

impl LogsCollectorTransport {
    pub async fn run(mut self, cancel_token: CancellationToken) {
        log::info!("Starting logs collector P2P transport");
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                ev = self.swarm.select_next_some() => self.on_swarm_event(ev),
                Some(logs_collected) = self.logs_collected_rx.recv() => self.swarm.behaviour_mut().logs_collected(logs_collected),
            }
        }
        log::info!("Shutting down logs collector P2P transport");
    }

    fn on_swarm_event(&mut self, ev: SwarmEvent<LogsCollectorEvent>) {
        log::trace!("Swarm event: {ev:?}");
        record_event(&ev);
        if let SwarmEvent::Behaviour(ev) = ev {
            self.events_tx.send_lossy(ev)
        }
    }
}

#[derive(Clone)]
pub struct LogsCollectorTransportHandle {
    logs_collected_tx: Sender<LogsCollected>,
    _task_manager: Arc<TaskManager>,
}

impl LogsCollectorTransportHandle {
    fn new(
        logs_collected_tx: Sender<LogsCollected>,
        transport: LogsCollectorTransport,
        shutdown_timeout: Duration,
    ) -> Self {
        let mut task_manager = TaskManager::new(shutdown_timeout);
        task_manager.spawn(|c| transport.run(c));
        Self {
            logs_collected_tx,
            _task_manager: Arc::new(task_manager),
        }
    }

    pub fn logs_collected(&self, logs_collected: LogsCollected) -> Result<(), QueueFull> {
        log::debug!("Queueing LogsCollected message: {logs_collected:?}");
        self.logs_collected_tx.try_send(logs_collected)
    }
}

pub fn start_transport(
    swarm: Swarm<Wrapped<LogsCollectorBehaviour>>,
    config: LogsCollectorConfig,
) -> (impl Stream<Item = LogsCollectorEvent>, LogsCollectorTransportHandle) {
    let (logs_collected_tx, logs_collected_rx) =
        new_queue(config.logs_collected_queue_size, "logs_collected");
    let (events_tx, events_rx) = new_queue(config.events_queue_size, "events");
    let transport = LogsCollectorTransport {
        swarm,
        logs_collected_rx,
        events_tx,
    };
    let handle =
        LogsCollectorTransportHandle::new(logs_collected_tx, transport, config.shutdown_timeout);
    (events_rx, handle)
}
