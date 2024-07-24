use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures_core::Stream;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent, ToSwarm};
use libp2p::Swarm;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use contract_client::PeerId;

use crate::behaviour::base::{BaseBehaviour, BaseBehaviourEvent};
use crate::behaviour::wrapped::{BehaviourWrapper, TToSwarm, Wrapped};
use crate::record_event;
use crate::util::{new_queue, Sender, TaskManager, DEFAULT_SHUTDOWN_TIMEOUT};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ping {
    pub peer_id: PeerId,
    pub ping: subsquid_messages::Ping,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PingsCollectorConfig {
    pub events_queue_size: usize,
    pub shutdown_timeout: Duration,
}

impl Default for PingsCollectorConfig {
    fn default() -> Self {
        Self {
            events_queue_size: 1000,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

pub struct PingsCollectorBehaviour {
    base: Wrapped<BaseBehaviour>,
}

impl PingsCollectorBehaviour {
    pub fn new(mut base: BaseBehaviour) -> Wrapped<Self> {
        base.subscribe_pings();
        Self { base: base.into() }.into()
    }
}

impl BehaviourWrapper for PingsCollectorBehaviour {
    type Inner = Wrapped<BaseBehaviour>;
    type Event = Ping;

    fn inner(&mut self) -> &mut Self::Inner {
        &mut self.base
    }

    fn on_inner_event(
        &mut self,
        ev: <Self::Inner as NetworkBehaviour>::ToSwarm,
    ) -> impl IntoIterator<Item = TToSwarm<Self>> {
        match ev {
            BaseBehaviourEvent::Ping { peer_id, ping } => {
                log::debug!("Got ping from {peer_id}: {ping:?}");
                Some(ToSwarm::GenerateEvent(Ping { peer_id, ping }))
            }
            _ => None,
        }
    }
}

struct PingsCollectorTransport {
    swarm: Swarm<Wrapped<PingsCollectorBehaviour>>,
    pings_tx: Sender<Ping>,
}

impl PingsCollectorTransport {
    pub async fn run(mut self, cancel_token: CancellationToken) {
        log::info!("Starting pings collector P2P transport");
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                ev = self.swarm.select_next_some() => self.on_swarm_event(ev),
            }
        }
        log::info!("Shutting down pings collector P2P transport");
    }

    fn on_swarm_event(&mut self, ev: SwarmEvent<Ping>) {
        log::trace!("Swarm event: {ev:?}");
        record_event(&ev);
        if let SwarmEvent::Behaviour(ping) = ev {
            self.pings_tx.send_lossy(ping)
        }
    }
}

#[derive(Clone)]
pub struct PingsCollectorTransportHandle {
    _task_manager: Arc<TaskManager>,
}

impl PingsCollectorTransportHandle {
    fn new(transport: PingsCollectorTransport, shutdown_timeout: Duration) -> Self {
        let mut task_manager = TaskManager::new(shutdown_timeout);
        task_manager.spawn(|c| transport.run(c));
        Self {
            _task_manager: Arc::new(task_manager),
        }
    }
}

pub fn start_transport(
    swarm: Swarm<Wrapped<PingsCollectorBehaviour>>,
    config: PingsCollectorConfig,
) -> (impl Stream<Item = Ping>, PingsCollectorTransportHandle) {
    let (pings_tx, pings_rx) = new_queue(config.events_queue_size, "events");
    let transport = PingsCollectorTransport { swarm, pings_tx };
    let handle = PingsCollectorTransportHandle::new(transport, config.shutdown_timeout);
    (pings_rx, handle)
}
