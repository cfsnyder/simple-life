use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

pub type StopFn = Box<dyn Stop>;

#[async_trait]
pub trait Stop {
    async fn stop(self);
}

#[async_trait]
pub trait Lifecycle {
    async fn start(self: Arc<Self>) -> Option<StopFn>;
}

#[async_trait]
impl<F, R> Stop for F
where
    F: FnOnce() -> R + Send,
    R: Future<Output = ()> + Send,
{
    async fn stop(self) {
        self().await
    }
}

pub fn interval_worker<S, F, R>(s: Arc<S>, period: Duration, fun: F) -> StopFn
where
    S: Send + Sync + 'static,
    F: Fn(Arc<S>) -> R + Send + Sync + 'static,
    R: Future<Output = ()> + Send + Sync,
{
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    let jh = tokio::spawn(async move {
        let sleep = tokio::time::sleep(period);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = &mut sleep => {
                    fun(s.clone()).await;
                    sleep.as_mut().reset(tokio::time::Instant::now() + period);
                },
                _ = &mut rx => {
                    return;
                }
            }
        }
    });
    Box::new(|| {
        Box::pin(async move {
            let _ = tx.send(());
            jh.await.unwrap();
        })
    })
}
