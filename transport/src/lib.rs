// subsquid-network-transport, the transport layer for the Subsquid network.
// Copyright (C) 2024 Subsquid Labs GmbH

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use libp2p::{noise, swarm::DialError, TransportError};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tokio::sync::mpsc;

pub use libp2p::{
    identity::{Keypair, ParseError as IdParseError, PublicKey},
    Multiaddr, PeerId,
};

#[cfg(feature = "metrics")]
pub use libp2p::metrics::{Metrics, Recorder};
#[cfg(feature = "metrics")]
pub use prometheus_client::registry::Registry;

#[cfg(feature = "actors")]
mod actors;
#[cfg(feature = "actors")]
mod behaviour;
#[cfg(feature = "actors")]
mod builder;
mod cli;
#[cfg(feature = "proto")]
mod codec;
#[cfg(feature = "metrics")]
pub mod metrics;
pub mod protocol;
pub mod util;

#[cfg(feature = "gateway")]
pub use crate::actors::gateway::{
    GatewayBehaviour, GatewayConfig, GatewayEvent, GatewayTransportHandle,
};
#[cfg(feature = "logs-collector")]
pub use crate::actors::logs_collector::{
    LogsCollectorBehaviour, LogsCollectorConfig, LogsCollectorEvent, LogsCollectorTransportHandle,
};
#[cfg(feature = "observer")]
pub use crate::actors::observer::{
    ObserverBehaviour, ObserverConfig, ObserverEvent, ObserverTransportHandle,
};
#[cfg(feature = "pings-collector")]
pub use crate::actors::pings_collector::{
    Ping, PingsCollectorBehaviour, PingsCollectorConfig, PingsCollectorTransportHandle,
};
#[cfg(feature = "scheduler")]
pub use crate::actors::scheduler::{
    SchedulerBehaviour, SchedulerConfig, SchedulerEvent, SchedulerTransportHandle,
};
#[cfg(feature = "worker")]
pub use crate::actors::worker::{
    WorkerBehaviour, WorkerConfig, WorkerEvent, WorkerTransportHandle,
};
#[cfg(feature = "actors")]
pub use builder::P2PTransportBuilder;
pub use cli::{BootNode, TransportArgs};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicConfig {
    pub mtu_discovery_max: u16,
    pub keep_alive_interval_ms: u32,
    pub max_idle_timeout_ms: u32,
}

fn parse_var<T: FromStr>(var: &str, default: T) -> T {
    std::env::var(var).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

impl QuicConfig {
    pub fn from_env() -> Self {
        let mtu_discovery_max = parse_var("MTU_DISCOVERY_MAX", 1452);
        let keep_alive_interval_ms = parse_var("KEEP_ALIVE_INTERVAL_MS", 5000);
        let max_idle_timeout_ms = parse_var("MAX_IDLE_TIMEOUT_MS", 60000);
        Self {
            mtu_discovery_max,
            keep_alive_interval_ms,
            max_idle_timeout_ms,
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Queue full")]
pub struct QueueFull;

impl<T> From<mpsc::error::TrySendError<T>> for QueueFull {
    fn from(_: mpsc::error::TrySendError<T>) -> Self {
        Self
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Libp2p transport creation failed: {0}")]
    Transport(String),
    #[error("Listening failed: {0:?}")]
    Listen(#[from] TransportError<std::io::Error>),
    #[error("Dialing failed: {0:?}")]
    Dial(#[from] DialError),
    #[error("{0}")]
    Contract(#[from] contract_client::ClientError),
}

impl From<noise::Error> for Error {
    fn from(e: noise::Error) -> Self {
        Self::Transport(e.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Transport(e.to_string())
    }
}

#[cfg(feature = "actors")]
#[cfg(feature = "metrics")]
pub(crate) fn record_event<T>(event: &T)
where
    Metrics: Recorder<T>,
{
    if let Some(metrics) = metrics::LIBP2P_METRICS.get() {
        metrics.record(event);
    }
}

#[cfg(feature = "actors")]
#[cfg(not(feature = "metrics"))]
pub(crate) fn record_event<T>(_event: T) {}
