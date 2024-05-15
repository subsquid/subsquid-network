use std::{
    fmt::Debug,
    future::Future,
    pin::{pin, Pin},
    task::{Context, Poll},
};

use futures_core::Stream;
use tokio::sync::mpsc;

#[cfg(feature = "metrics")]
use crate::metrics::{DROPPED, QUEUE_SIZE};
use crate::QueueFull;

#[cfg(feature = "metrics")]
const QUEUE_NAME: &str = "queue_name";

#[derive(Clone)]
pub struct Sender<T> {
    inner: mpsc::Sender<T>,
    name: &'static str,
}

impl<T: Debug> Sender<T> {
    pub fn new(inner: mpsc::Sender<T>, name: &'static str) -> Self {
        Self { inner, name }
    }

    /// Lossy send. Drops the message if queue is full.
    pub fn send_lossy(&self, msg: T) {
        self.try_send(msg).unwrap_or_else(|e| {
            #[cfg(feature = "metrics")]
            DROPPED.get_or_create(&vec![(QUEUE_NAME, self.name)]).inc();
            log::warn!("Queue {} full. Message dropped: {e:?}", self.name);
        });
    }

    /// Try to send message. Returns `QueueFull` if queue is full
    pub fn try_send(&self, msg: T) -> Result<(), QueueFull> {
        self.inner.try_send(msg)?;
        #[cfg(feature = "metrics")]
        QUEUE_SIZE.get_or_create(&vec![(QUEUE_NAME, self.name)]).inc();
        Ok(())
    }
}

pub struct Receiver<T> {
    inner: mpsc::Receiver<T>,
    #[allow(dead_code)]
    name: &'static str,
}

impl<T> Receiver<T> {
    pub fn new(inner: mpsc::Receiver<T>, name: &'static str) -> Self {
        Self { inner, name }
    }

    pub async fn recv(&mut self) -> Option<T> {
        self.inner.recv().await.map(|msg| {
            #[cfg(feature = "metrics")]
            QUEUE_SIZE.get_or_create(&vec![(QUEUE_NAME, self.name)]).dec();
            msg
        })
    }
}

impl<T> Stream for Receiver<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        pin!(self.recv()).as_mut().poll(cx)
    }
}

pub fn new_queue<T: Debug>(size: usize, name: &'static str) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = mpsc::channel(size);
    let tx = Sender::new(tx, name);
    let rx = Receiver::new(rx, name);
    (tx, rx)
}
