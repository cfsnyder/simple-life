use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
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

pub fn seq<A, B>(a: A, b: B) -> impl Lifecycle
    where
        A: Lifecycle,
        B: Lifecycle,
{
    lifecycle!(state, {
        (a.start().await, b.start().await)
    }, {
        let (a_stop, b_stop) = state;
        b_stop.stop().await;
        a_stop.stop().await;
    })
}

pub fn parallel<A, B>(a: A, b: B) -> impl Lifecycle
    where
        A: Lifecycle,
        B: Lifecycle,
{
    lifecycle!(state, {
        tokio::join!(a.start(), b.start())
    }, {
        let (a_stop, b_stop) = state;
        let _ = tokio::join!(a_stop.stop(), b_stop.stop());
    })
}

#[macro_export]
macro_rules! parallel {
    ($x:expr $(,)?) => ($x);
    ($x:expr, $($y:expr),+ $(,)?) => (
        simple_life::parallel($x, simple_life::parallel!($($y),+))
    )
}

#[macro_export]
macro_rules! seq {
    ($x:expr $(,)?) => ($x);
    ($x:expr, $($y:expr),+ $(,)?) => (
        simple_life::seq($x, simple_life::seq!($($y),+))
    )
}

#[macro_export]
macro_rules! lifecycle {
    ($state:ident, $start:block, $stop:block) => (
        move || {
            async move {
                let $state = $start;
                move || {
                    async move {
                        $stop
                    }
                }
            }
        }
    );
    ($start:block, $stop:block) => (
        simple_life::lifecycle!(_state, $start, $stop)
    );
}

#[macro_export]
macro_rules! start {
    ($x:block) => (
        simple_life::lifecycle!($x, {})
    );
}

#[macro_export]
macro_rules! stop {
    ($x:block) => (
        simple_life::lifecycle!({}, $x)
    );
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

pub fn spawn_interval<S, F, R>(s: S, period: Duration, fun: F) -> impl Lifecycle
    where
        S: Clone + 'static + Send + Sync,
        F: Fn(S) -> R + Send + Sync + 'static,
        R: Future<Output=()> + Send,
{
    spawn_with_shutdown(move |mut sig| async move {
        let sleep = tokio::time::sleep(period);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                    _ = &mut sleep => {
                        fun(s.clone()).await;
                        sleep.as_mut().reset(tokio::time::Instant::now() + period);
                    },
                    _ = &mut sig => {
                        return;
                    }
                }
        }
    })
}

pub struct ShutdownSignal(tokio::sync::oneshot::Receiver<()>);

impl Future for ShutdownSignal {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|r| r.unwrap())
    }
}

pub fn spawn_with_shutdown<F, R>(fun: F) -> impl Lifecycle
    where
        F: FnOnce(ShutdownSignal) -> R + Send + 'static,
        R: Future<Output=()> + Send,
{
    lifecycle!(chans, {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let jh = tokio::spawn(async move {
             let _ = fun(ShutdownSignal(shutdown_rx)).await;
        });
        (shutdown_tx, jh)
    }, {
        let (shutdown_tx, jh) = chans;
        shutdown_tx.send(()).unwrap();
        let _ = jh.await;
    })
}

#[derive(Eq, PartialEq, Debug)]
pub struct NoStop;

#[async_trait]
impl Stop for NoStop {
    async fn stop(self) {}
}