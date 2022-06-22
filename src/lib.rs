use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;

pub type Stop = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output=()> + Send>>>;

#[async_trait]
pub trait Lifecycle {
    async fn start(self: Arc<Self>) -> Stop;
}

#[macro_export]
macro_rules! interval_worker {
    ($name:expr, $interval:expr, $body:expr) => {{
        if cfg!(feature = "tracing") {
            tracing::info!("{} starting", $name);
        }
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let jh = tokio::spawn(async move {
            let sleep = tokio::time::sleep($interval.to_std().unwrap());
            tokio::pin!(sleep);
            loop {
                tokio::select! {
                    _ = &mut sleep => {
                        $body.await;
                        sleep.as_mut().reset(tokio::time::Instant::now() + $interval.to_std().unwrap());
                    },
                    _ = &mut rx => {
                        return;
                    }
                }
            }
        });
        Box::new(move || {
            Box::pin(async move {
                if cfg!(feature = "tracing") {
                    tracing::info!("{} stopping", $name);
                }
                let _ = tx.send(());
                tokio::time::timeout(std::time::Duration::from_secs(60), jh)
                    .await
                    .unwrap()
                    .unwrap();
                if cfg!(feature = "tracing") {
                    tracing::info!("{} stopped", $name);
                }
            })
        })
    }};
}

#[macro_export]
macro_rules! no_teardown {
    () => {
        Box::new(|| Box::pin(async {}))
    };
}
