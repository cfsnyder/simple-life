use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;

#[async_trait]
pub trait Stop: Send {
    async fn stop(self);
}

#[async_trait]
pub trait Lifecycle: Send + 'static {
    type S: Stop;

    async fn start(self) -> Self::S;
}

pub fn combine<A, B>(a: A, b: B) -> impl Lifecycle
where
    A: Lifecycle,
    B: Lifecycle,
{
    move || {
        async move {
            let a_stop = a.start().await;
            let b_stop = b.start().await;
            move || {
                async {
                    a_stop.stop().await;
                    b_stop.stop().await;
                }
            }
        }
    }
}

#[async_trait]
impl<F, R, O> Lifecycle for F
    where
        F: FnOnce() -> R + 'static + Send,
        R: Future<Output=O> + Send,
        O: Stop,
{
    type S = O;

    async fn start(self) -> Self::S {
        self().await
    }
}

#[async_trait]
impl<F, R> Stop for F
    where
        F: FnOnce() -> R + Send,
        R: Future<Output=()> + Send,
{
    async fn stop(self) {
        self().await
    }
}

pub fn interval<S, F, R>(s: S, period: Duration, fun: F) -> impl Lifecycle
    where
        S: Clone + 'static + Send + Sync,
        F: Fn(S) -> R + Send + Sync + 'static,
        R: Future<Output=()> + Send,
{
    move || async move {
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
        move || {
            async move {
                let _ = tx.send(());
                jh.await.unwrap();
            }
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
pub struct NoStop;

#[async_trait]
impl Stop for NoStop {
    async fn stop(self) {
    }
}