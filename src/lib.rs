use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use tokio::signal;
use tokio::signal::unix::SignalKind;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::error::Elapsed;

#[async_trait]
pub trait Stop: Send {
    async fn stop(self);

    async fn concrete(self) -> ConcreteStop
    where
        Self: Sized + 'static,
    {
        ConcreteStop::new(self).await
    }

    async fn into_guard(self) -> StopGuard
    where
        Self: Sized + 'static,
    {
        StopGuard::new(self).await
    }
}

#[async_trait]
pub trait Lifecycle: Send + 'static {
    type S: Stop;

    async fn start(self) -> Self::S;

    async fn concrete(self) -> ConcreteLifecycle
    where
        Self: Sized,
    {
        ConcreteLifecycle::new(self).await
    }
}

pub struct StopGuard {
    _tx: oneshot::Sender<()>,
}

impl StopGuard {
    async fn new(stop: impl Stop + 'static) -> Self {
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = rx.await;
            let _ = stop.stop().await;
        });
        StopGuard { _tx: tx }
    }
}

pub struct IntrospectableStop {
    sig: oneshot::Sender<()>,
    jh: JoinHandle<()>,
}

impl IntrospectableStop {
    fn new(jh: JoinHandle<()>, sig: oneshot::Sender<()>) -> Self {
        IntrospectableStop { jh, sig }
    }

    pub fn is_finished(&self) -> bool {
        self.jh.is_finished()
    }
}

#[async_trait]
impl Stop for IntrospectableStop {
    async fn stop(self) {
        let _ = self.sig.send(());
        let _ = self.jh.await;
    }
}

pub struct ConcreteLifecycle {
    tx: oneshot::Sender<oneshot::Sender<ConcreteStop>>,
}

impl ConcreteLifecycle {
    async fn new(lc: impl Lifecycle) -> Self {
        let (tx, rx) = oneshot::channel::<oneshot::Sender<ConcreteStop>>();
        tokio::spawn(async move {
            if let Ok(stop_tx) = rx.await {
                let stop = lc.start().await;
                let _ = stop_tx.send(ConcreteStop::new(stop).await);
            }
        });
        ConcreteLifecycle { tx }
    }
}

pub async fn parallel_iter<I: IntoIterator<Item = ConcreteLifecycle>>(
    iter: I,
) -> ConcreteLifecycle {
    let mut lc = None;
    for next in iter.into_iter() {
        if let Some(old_lc) = lc {
            lc = Some(parallel(old_lc, next).concrete().await);
        } else {
            lc = Some(next);
        }
    }
    if let Some(lc) = lc {
        lc
    } else {
        NoLife.concrete().await
    }
}

pub struct ConcreteStop {
    tx: oneshot::Sender<oneshot::Sender<()>>,
}

impl ConcreteStop {
    async fn new(stop: impl Stop + 'static) -> ConcreteStop {
        let (tx, rx) = oneshot::channel::<oneshot::Sender<()>>();
        tokio::spawn(async move {
            if let Ok(done_tx) = rx.await {
                stop.stop().await;
                let _ = done_tx.send(());
            }
        });
        ConcreteStop { tx }
    }
}

#[async_trait]
impl Lifecycle for ConcreteLifecycle {
    type S = ConcreteStop;
    async fn start(self) -> Self::S {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(tx);
        rx.await.unwrap()
    }
}

#[async_trait]
impl Stop for ConcreteStop {
    async fn stop(self) {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(tx);
        rx.await.unwrap();
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

pub fn spawn_interval<S, F, R>(
    s: S,
    period: Duration,
    fun: F,
) -> impl Lifecycle<S = IntrospectableStop>
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

pub fn spawn_with_delay<F, R>(delay: Duration, fun: F) -> impl Lifecycle<S = IntrospectableStop>
where
    F: Fn() -> R + Send + Sync + 'static,
    R: Future<Output = ()> + Send,
{
    spawn_with_shutdown(move |mut sig| async move {
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);
        tokio::select! {
            _ = &mut sleep => {
                fun().await;
            },
            _ = &mut sig => {
                return;
            }
        }
    })
}

pub fn spawn_lifecycle_with_delay(
    delay: Duration,
    lc: impl Lifecycle,
) -> impl Lifecycle<S = IntrospectableStop> {
    spawn_with_shutdown(move |mut sig| async move {
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);
        tokio::select! {
            _ = &mut sleep => {
                let stopper = lc.start().await;
                sig.await;
                stopper.stop().await;
            },
            _ = &mut sig => {
                return;
            }
        }
    })
}

pub struct ShutdownSignal(tokio::sync::oneshot::Receiver<()>);

impl Future for ShutdownSignal {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|_r| ())
    }
}

pub fn spawn_with_shutdown<F, R>(fun: F) -> impl Lifecycle<S = IntrospectableStop>
where
    F: FnOnce(ShutdownSignal) -> R + Send + 'static,
    R: Future<Output = ()> + Send,
{
    move || async move {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let jh = tokio::spawn(async move {
            let _ = fun(ShutdownSignal(shutdown_rx)).await;
        });
        IntrospectableStop::new(jh, shutdown_tx)
    }
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
    tx: Sender<ConcreteLifecycle>,
}

impl LazyStarter {
    fn new() -> (impl Lifecycle<S = IntrospectableStop>, LazyStarter) {
        let (tx, rx) = tokio::sync::mpsc::channel(5);
        (LazyStarter::lifecycle(rx), LazyStarter { tx })
    }

    fn lifecycle(mut rx: Receiver<ConcreteLifecycle>) -> impl Lifecycle<S = IntrospectableStop> {
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
                            let _ = sig.await;
                            break;
                        }
                    },
                }
            }
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
        let _ = self.tx.send(life.concrete().await).await;
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

pub struct NoLife;

#[async_trait]
impl Lifecycle for NoLife {
    type S = NoStop;

    async fn start(self) -> Self::S {
        NoStop
    }
}
