use std::future::Future;
use std::time::Duration;

use tokio::runtime::RuntimeFlavor;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub use tokio_util::sync::CancellationToken;

pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// A manager which allows spawning cancellable background tasks
/// and makes sure all them are cancelled when it is dropped.
pub struct TaskManager {
    shutdown_timeout: Duration,
    cancel_token: CancellationToken,
    tasks: Vec<JoinHandle<()>>, // This can technically grow unbounded.
}

impl Default for TaskManager {
    fn default() -> Self {
        Self::new(DEFAULT_SHUTDOWN_TIMEOUT)
    }
}

impl TaskManager {
    /// Create a new TaskManager with the given `shutdown_timeout`. The timeout specifies
    /// how much time tasks spawn by this manager will have to finish when they get cancelled.
    /// Panics if called outside tokio runtime or when the runtime flavour is `CurrentThread`.
    pub fn new(shutdown_timeout: Duration) -> Self {
        // Current thread runtime doesn't allow async drop (see `Drop` impl below)
        assert_ne!(
            tokio::runtime::Handle::current().runtime_flavor(),
            RuntimeFlavor::CurrentThread,
            "Current thread runtime not supported"
        );

        Self {
            shutdown_timeout,
            cancel_token: CancellationToken::new(),
            tasks: vec![],
        }
    }

    /// Spawn a new task. A cancellation token will be passed to the constructor.
    /// When cancelled, the task is supposed to finish within the `shutdown_timeout`.
    /// Panics if `.cancel()` or `.await_stop()` has been already called.
    pub fn spawn<F, T>(&mut self, f: F)
    where
        F: FnOnce(CancellationToken) -> T,
        T: Future<Output = ()> + Send + 'static,
    {
        assert!(!self.cancel_token.is_cancelled());
        let child_token = self.cancel_token.child_token();
        let future = f(child_token);
        self.tasks.push(tokio::spawn(future));
    }

    /// Spawn a new periodic task that will be run every `interval`. First run will occur
    /// after `interval` from spawning, not immediately.
    /// Task can break its driving loop by calling `.cancel()` on the token passed to it.
    /// When cancelled, the task is supposed to finish within the `shutdown_timeout`.
    /// Panics if `.cancel()` or `.await_stop()` has been already called.
    pub fn spawn_periodic<F, T>(&mut self, mut f: F, interval: Duration)
    where
        F: FnMut(CancellationToken) -> T,
        F: Send + 'static,
        T: Future<Output = ()> + Send,
    {
        let mut interval = tokio::time::interval_at(Instant::now() + interval, interval);
        self.spawn(|cancel_token| async move {
            loop {
                tokio::select! {
                    _ = interval.tick() => (),
                    _ = cancel_token.cancelled() => break,
                };
                f(cancel_token.clone()).await;
            }
        })
    }

    /// Cancel all spawned tasks.
    pub fn cancel(&self) {
        self.cancel_token.cancel()
    }

    /// Cancel all spawned tasks and wait for them to finish. This is done automatically
    /// when the `TaskManager` is dropped.
    pub async fn await_stop(&mut self) {
        self.cancel(); // Just in case self.cancel() wasn't called earlier
        let results = futures::future::join_all(
            self.tasks
                .drain(..)
                .map(|handle| tokio::time::timeout(self.shutdown_timeout, handle)),
        )
        .await;
        for result in results {
            match result {
                Ok(Ok(())) => (),
                Ok(Err(e)) => log::error!("Error joining task: {e:?}"),
                Err(_) => log::error!("Stopping task timed out"),
            }
        }
    }
}

impl Drop for TaskManager {
    // Implemented according to https://github.com/tokio-rs/tokio/issues/5843
    fn drop(&mut self) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.await_stop());
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test(flavor = "current_thread")]
    #[should_panic]
    async fn test_current_thread() {
        TaskManager::default();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multi_threaded() {
        let task = move |flag: Arc<Mutex<bool>>, cancel_delay: Duration| {
            move |cancel_token: CancellationToken| async move {
                cancel_token.cancelled().await;
                tokio::time::sleep(cancel_delay).await;
                *flag.lock().await = true;
            }
        };

        let shutdown_timeout = Duration::from_millis(20);
        let mut task_manager = TaskManager::new(shutdown_timeout);

        let task1_stopped = Arc::new(Mutex::new(false));
        let task2_stopped = Arc::new(Mutex::new(false));

        task_manager.spawn(task(task1_stopped.clone(), Duration::from_millis(0)));
        task_manager.spawn(task(task2_stopped.clone(), Duration::from_millis(1000)));

        // Tasks should be running
        assert!(!*task1_stopped.lock().await);
        assert!(!*task2_stopped.lock().await);

        // Dropping the manager should cancel tasks
        drop(task_manager);
        tokio::time::sleep(shutdown_timeout).await;

        // Task2 has long cancel delay, so task2_stopped == false
        assert!(*task1_stopped.lock().await);
        assert!(!*task2_stopped.lock().await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_periodic() {
        let counter1 = Arc::new(Mutex::new(0));
        let counter2 = counter1.clone();

        let task = move |cancel_token: CancellationToken| {
            let counter = counter2.clone();
            async move {
                let mut counter = counter.lock().await;
                *counter += 1;
                if *counter >= 2 {
                    cancel_token.cancel();
                }
            }
        };

        let interval = Duration::from_millis(20);
        let mut task_manager = TaskManager::default();
        task_manager.spawn_periodic(task, interval);

        // Task should get cancelled after second run and only execute twice
        tokio::time::sleep(interval * 4).await;
        assert_eq!(*counter1.lock().await, 2);
    }
}
