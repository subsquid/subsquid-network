use derivative::Derivative;
use libp2p::{
    gossipsub::{
        self, MessageAcceptance, MessageAuthenticity, PublishError, Sha256Topic, TopicHash,
    },
    identity::Keypair,
    swarm::{NetworkBehaviour, ToSwarm},
};
use std::cmp::max;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::Instant;

use crate::{
    behaviour::wrapped::{BehaviourWrapper, TToSwarm},
    record_event, PeerId,
};

#[cfg(feature = "metrics")]
use crate::metrics::DISCARDED_MESSAGES;

const SUBSCRIPTION_TIMEOUT: Duration = Duration::from_secs(60);

struct TopicState {
    name: &'static str,
    topic: Sha256Topic,
    peer_states: HashMap<PeerId, PeerState>,
    validation_config: MsgValidationConfig,
    subscribed_at: Instant,
}

pub struct MsgValidationConfig {
    /// Minimum interval between messages from the same origin
    pub min_interval: Duration,
    /// Maximum messages per min_interval
    pub max_burst: u32,
    /// How far back from the latest seq_no is accepted
    pub keep_last: u64,
}

impl MsgValidationConfig {
    pub fn new(min_interval: Duration) -> Self {
        Self {
            min_interval,
            max_burst: 1,
            keep_last: 1,
        }
    }

    pub fn max_burst(mut self, max_burst: u32) -> Self {
        self.max_burst = max_burst;
        self
    }

    pub fn keep_last(mut self, keep_last: u64) -> Self {
        self.keep_last = keep_last;
        self
    }
}

struct PeerState {
    last_seq_no: u64,
    last_time: Instant,
    burst: u32,
}

impl PeerState {
    pub fn new(seq_no: u64) -> Self {
        Self {
            last_seq_no: seq_no,
            last_time: Instant::now(),
            burst: 1,
        }
    }

    pub fn validate_msg(
        &mut self,
        seq_no: u64,
        config: &MsgValidationConfig,
    ) -> Result<(), &'static str> {
        // Sequence numbers should be timestamp-based, can't be from the future
        if seq_no == 0 || seq_no > timestamp_now() {
            return Err("invalid sequence number");
        }
        if seq_no + config.keep_last <= self.last_seq_no {
            return Err("old message");
        }

        if self.last_time.elapsed() < config.min_interval {
            self.burst += 1;
        } else {
            self.last_time = Instant::now();
            self.burst = 1;
        }
        if self.burst > config.max_burst {
            return Err("too many messages within min_interval");
        }

        self.last_seq_no = max(self.last_seq_no, seq_no);
        Ok(())
    }
}

impl TopicState {
    pub fn new(name: &'static str, validation_config: MsgValidationConfig) -> Self {
        Self {
            name,
            topic: Sha256Topic::new(name),
            peer_states: Default::default(),
            validation_config,
            subscribed_at: Instant::now(),
        }
    }

    pub fn validate_msg(&mut self, peer_id: PeerId, seq_no: u64) -> Result<(), &'static str> {
        match self.peer_states.get_mut(&peer_id) {
            None => {
                self.peer_states.insert(peer_id, PeerState::new(seq_no));
            }
            Some(state) => state.validate_msg(seq_no, &self.validation_config)?,
        }
        Ok(())
    }
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct PubsubMsg {
    pub peer_id: PeerId,
    pub topic: &'static str,
    #[derivative(Debug = "ignore")]
    pub data: Box<[u8]>,
}

pub struct PubsubBehaviour {
    inner: gossipsub::Behaviour,
    topics: HashMap<TopicHash, TopicState>,
}

impl PubsubBehaviour {
    pub fn new(keypair: Keypair, max_msg_size: usize) -> Self {
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .validate_messages()
            .message_id_fn(msg_id)
            .max_transmit_size(max_msg_size)
            .build()
            .expect("config should be valid");
        let inner =
            gossipsub::Behaviour::new(MessageAuthenticity::Signed(keypair), gossipsub_config)
                .expect("config should be valid");
        Self {
            inner,
            topics: Default::default(),
        }
    }

    pub fn subscribe(&mut self, topic_name: &'static str, validation_config: MsgValidationConfig) {
        log::info!("Subscribing to topic {topic_name}");
        let topic = TopicState::new(topic_name, validation_config);
        let topic_hash = topic.topic.hash();
        if self.topics.contains_key(&topic_hash) {
            log::warn!("Topic {topic_name} already subscribed");
            return;
        }
        if let Err(e) = self.inner.subscribe(&topic.topic) {
            log::error!("Cannot subscribe to {topic_name}: {e:}");
            return;
        }
        self.topics.insert(topic_hash, topic);
        log::info!("Topic {topic_name} subscribed");
    }

    pub fn publish(&mut self, topic_name: &'static str, msg: impl Into<Vec<u8>>) {
        log::debug!("Publishing message to topic {topic_name}");
        let topic_hash = Sha256Topic::new(topic_name).hash();
        let Some(topic) = self.topics.get(&topic_hash) else {
            return log::error!("Cannot publish to unsubscribed topic: {topic_name}");
        };

        match self.inner.publish(topic_hash, msg) {
            Err(PublishError::InsufficientPeers)
                if topic.subscribed_at.elapsed() <= SUBSCRIPTION_TIMEOUT =>
            {
                log::info!("Waiting for peers to publish to {topic_name}")
            }
            Err(e) => log::error!("Error publishing message to {topic_name}: {e:?}"),
            Ok(_) => log::debug!("Message published to {topic_name}"),
        }
    }

    /// Validate gossipsub message
    ///   1) Check if message is not anonymous,
    ///   2) Check if topic is known (subscribed),
    ///   3) Enforce message ordering (if configured for topic).
    fn validate_gossipsub_msg(
        &mut self,
        msg: gossipsub::Message,
    ) -> Result<PubsubMsg, &'static str> {
        let Some(peer_id) = msg.source else {
            return Err("anonymous message");
        };
        let Some(seq_no) = msg.sequence_number else {
            return Err("message without sequence number");
        };
        let Some(topic_state) = self.topics.get_mut(&msg.topic) else {
            return Err("message with unknown topic");
        };
        topic_state.validate_msg(peer_id, seq_no)?;

        Ok(PubsubMsg {
            peer_id,
            topic: topic_state.name,
            data: msg.data.into_boxed_slice(),
        })
    }
}

impl BehaviourWrapper for PubsubBehaviour {
    type Inner = gossipsub::Behaviour;
    type Event = PubsubMsg;

    fn inner(&mut self) -> &mut Self::Inner {
        &mut self.inner
    }

    fn on_inner_event(
        &mut self,
        ev: <Self::Inner as NetworkBehaviour>::ToSwarm,
    ) -> impl IntoIterator<Item = TToSwarm<Self>> {
        log::debug!("Gossipsub event received: {ev:?}");
        record_event(&ev);
        let gossipsub::Event::Message {
            message,
            propagation_source,
            message_id,
        } = ev
        else {
            return None;
        };

        match self.validate_gossipsub_msg(message) {
            Ok(msg) => {
                let _ = self.inner.report_message_validation_result(
                    &message_id,
                    &propagation_source,
                    MessageAcceptance::Accept,
                );
                Some(ToSwarm::GenerateEvent(msg))
            }
            Err(e) => {
                log::debug!("Discarding gossipsub message from {propagation_source}: {e}");
                #[cfg(feature = "metrics")]
                DISCARDED_MESSAGES.inc();
                let _ = self.inner.report_message_validation_result(
                    &message_id,
                    &propagation_source,
                    MessageAcceptance::Reject,
                );
                None
            }
        }
    }
}

// Default gossipsub msg ID function, copied from libp2p
fn msg_id(msg: &gossipsub::Message) -> gossipsub::MessageId {
    let mut source_string = if let Some(peer_id) = msg.source.as_ref() {
        peer_id.to_base58()
    } else {
        PeerId::from_bytes(&[0, 1, 0]).expect("Valid peer id").to_base58()
    };
    source_string.push_str(&msg.sequence_number.unwrap_or_default().to_string());
    gossipsub::MessageId::from(source_string)
}

#[inline(always)]
fn timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("we're after 1970")
        .as_nanos()
        .try_into()
        .expect("not that far in the future")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_throttle() -> anyhow::Result<()> {
        let config = MsgValidationConfig::new(Duration::from_secs(10));
        let mut peer_state = PeerState::new(1);

        // Only 1 message per 10s allowed
        assert!(peer_state.validate_msg(2, &config).is_err());
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(peer_state.validate_msg(2, &config).is_ok());

        // Old message ID not allowed
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(peer_state.validate_msg(1, &config).is_err());

        // 3-message burst allowed
        tokio::time::advance(Duration::from_secs(10)).await;
        let config = config.max_burst(3);
        assert!(peer_state.validate_msg(3, &config).is_ok());
        assert!(peer_state.validate_msg(4, &config).is_ok());
        assert!(peer_state.validate_msg(5, &config).is_ok());
        assert!(peer_state.validate_msg(6, &config).is_err());

        // Allow messages up to 5 behind last
        tokio::time::advance(Duration::from_secs(10)).await;
        let config = config.keep_last(5);
        assert!(peer_state.validate_msg(10, &config).is_ok());
        assert!(peer_state.validate_msg(6, &config).is_ok());
        assert!(peer_state.validate_msg(5, &config).is_err());

        Ok(())
    }
}
