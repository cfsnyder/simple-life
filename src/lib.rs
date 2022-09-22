use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use tokio::signal;
use tokio::signal::unix::SignalKind;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::error::Elapsed;

pub type BoxedStop = Box<dyn Stop>;
pub type BoxedLifecycle = Box<dyn Lifecycle<S = BoxedStop>>;

#[async_trait]
pub trait Stop: Send {
    async fn stop(self);

    fn boxed(self) -> BoxedStop
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

#[async_trait]
pub trait Lifecycle: Send + 'static {
    type S: Stop;

    async fn start(self) -> Self::S;

    fn boxed(self) -> BoxedLifecycle
    where
        Self: Sized,
    {
        Box::new(move || async move { self.start().await.boxed() })
    }
}

#[async_trait]
impl Lifecycle for BoxedLifecycle {
    type S = Box<dyn Stop>;

    async fn start(self) -> Self::S {
        Box::new(self.start().await)
    }
}

#[async_trait]
impl Stop for BoxedStop {
    async fn stop(self) {
        self.stop().await;
    }
}

pub fn seq<A, B>(a: A, b: B) -> impl Lifecycle
where
    A: Lifecycle,
    B: Lifecycle,
{
    lifecycle!(state, { (a.start().await, b.start().await) }, {
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
    lifecycle!(state, { tokio::join!(a.start(), b.start()) }, {
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
    (mut $state:ident, $start:block, $stop:block) => {
        move || async move {
            let mut $state = $start;
            move || async move { $stop }
        }
    };
    ($state:ident, $start:block, $stop:block) => {
        move || async move {
            let $state = $start;
            move || async move { $stop }
        }
    };
    ($start:block, $stop:block) => {
        simple_life::lifecycle!(_state, $start, $stop)
    };
}

#[macro_export]
macro_rules! start {
    ($x:block) => {
        simple_life::lifecycle!($x, {})
    };
}

#[macro_export]
macro_rules! stop {
    ($x:block) => {
        simple_life::lifecycle!({}, $x)
    };
}

#[async_trait]
impl<F, R, O> Lifecycle for F
where
    F: FnOnce() -> R + 'static + Send,
    R: Future<Output = O> + Send,
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
    R: Future<Output = ()> + Send,
{
    async fn stop(self) {
        self().await
    }
}

pub fn spawn_interval<S, F, R>(s: S, period: Duration, fun: F) -> impl Lifecycle
where
    S: Clone + 'static + Send + Sync,
    F: Fn(S) -> R + Send + Sync + 'static,
    R: Future<Output = ()> + Send,
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
    R: Future<Output = ()> + Send,
{
    lifecycle!(
        chans,
        {
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let jh = tokio::spawn(async move {
                let _ = fun(ShutdownSignal(shutdown_rx)).await;
            });
            (shutdown_tx, jh)
        },
        {
            let (shutdown_tx, jh) = chans;
            shutdown_tx.send(()).unwrap();
            let _ = jh.await;
        }
    )
}

pub async fn run_until_shutdown_sig(
    life: impl Lifecycle,
    timeout: Duration,
) -> Result<(), Elapsed> {
    let stopper = life.start().await;
    std_unix_shutdown_sigs().await;
    tokio::time::timeout(timeout, stopper.stop()).await
}

async fn std_unix_shutdown_sigs() {
    let mut kill_sig = signal::unix::signal(SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = signal::ctrl_c() => {},
        _ = kill_sig.recv() => {},
    }
}

#[derive(Clone)]
pub struct LazyStarter {
    tx: Sender<Box<dyn Lifecycle<S = Box<dyn Stop>>>>,
}

impl LazyStarter {
    fn new() -> (impl Lifecycle, LazyStarter) {
        let (tx, rx) = tokio::sync::mpsc::channel(5);
        (LazyStarter::lifecycle(rx), LazyStarter { tx })
    }

    fn lifecycle(mut rx: Receiver<BoxedLifecycle>) -> impl Lifecycle {
        spawn_with_shutdown(|sig| async move {
            let mut stoppers = vec![];
            tokio::pin!(sig);
            loop {
                tokio::select! {
                    _ = &mut sig => {
                        break;
                    },
                    lc = rx.recv() => {
                        if let Some(lc) = lc {
                            stoppers.push(lc.start().await);
                        } else {
                            break;
                        }
                    },
                }
            }
            let _ = sig.await;
            if let Some(fut) = stoppers.into_iter().map(Stop::stop).reduce(|a, b| {
                Box::pin(async {
                    tokio::join!(a, b);
                })
            }) {
                fut.await;
            }
        })
    }

    pub async fn start(&self, life: impl Lifecycle) {
        let _ = self.tx.send(life.boxed()).await;
    }
}

pub fn lazy_start() -> (impl Lifecycle, LazyStarter) {
    LazyStarter::new()
}

#[derive(Eq, PartialEq, Debug)]
pub struct NoStop;

#[async_trait]
impl Stop for NoStop {
    async fn stop(self) {}
}
