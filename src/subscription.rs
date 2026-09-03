//! Subscriptions: framed data from named streams and materialized views
//! (plan 03 Tasks 2.1/2.2).
//!
//! A dedicated reader task owns the `SubscriptionPortal` and forwards owned
//! frame messages over a bounded channel; cancellation runs through a
//! `CancellationToken` that the reader selects on (a portal blocked in
//! `next_frame` holds `&mut self` and cannot be closed from outside).
//! `PortalFrame::Batch` leases are released as soon as the owned
//! `RecordBatch` is extracted — frames convert eagerly and are never
//! borrowed across the boundary.

use std::sync::atomic::{AtomicBool, Ordering};

use arrow::datatypes::SchemaRef;
use laminar_db::subscription::{PortalFrame, SubscriptionPortal};
use napi::threadsafe_function::ThreadsafeFunction;

use crate::spawn;
use napi::{Error, Result};
use napi_derive::napi;
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio_util::sync::CancellationToken;

use crate::error::{coded_error, map_db_error};
use crate::query::{field_infos, ArrowBatch, FieldInfo};

/// Frames crossing from the reader task, in arrival order. `Lagged`,
/// `Failed`, and `Closed` are terminal.
pub(crate) enum FrameMsg {
    Data {
        batch: arrow::array::RecordBatch,
        sequence: u64,
    },
    Barrier {
        sequence: u64,
        epoch: u64,
        checkpoint_id: u64,
        through_sequence: u64,
    },
    Lagged(u64),
    Failed(String),
    Closed,
}

/// One frame from a subscription: `kind` is `'data'` (carries `batch`) or
/// `'barrier'` (checkpoint progress).
#[napi]
pub struct SubscriptionFrame {
    kind: String,
    batch: Option<ArrowBatch>,
    sequence: u64,
    epoch: Option<u64>,
    checkpoint_id: Option<u64>,
    through_sequence: Option<u64>,
}

impl SubscriptionFrame {
    fn data(batch: arrow::array::RecordBatch, sequence: u64) -> Self {
        Self {
            kind: "data".to_owned(),
            batch: Some(ArrowBatch::from(batch)),
            sequence,
            epoch: None,
            checkpoint_id: None,
            through_sequence: None,
        }
    }

    fn barrier(sequence: u64, epoch: u64, checkpoint_id: u64, through_sequence: u64) -> Self {
        Self {
            kind: "barrier".to_owned(),
            batch: None,
            sequence,
            epoch: Some(epoch),
            checkpoint_id: Some(checkpoint_id),
            through_sequence: Some(through_sequence),
        }
    }

    fn from_msg(msg: FrameMsg) -> Option<Self> {
        match msg {
            FrameMsg::Data { batch, sequence } => Some(Self::data(batch, sequence)),
            FrameMsg::Barrier {
                sequence,
                epoch,
                checkpoint_id,
                through_sequence,
            } => Some(Self::barrier(
                sequence,
                epoch,
                checkpoint_id,
                through_sequence,
            )),
            // Terminal messages surface as errors in `nextFrame`; they never
            // materialize a frame.
            FrameMsg::Lagged(_) | FrameMsg::Failed(_) | FrameMsg::Closed => None,
        }
    }
}

#[napi]
impl SubscriptionFrame {
    /// `'data'` or `'barrier'`.
    #[napi(getter)]
    pub fn kind(&self) -> String {
        self.kind.clone()
    }

    /// The data batch; `undefined` for barriers.
    #[napi(getter)]
    pub fn batch(&self) -> Option<ArrowBatch> {
        self.batch.as_ref().map(|batch| batch.share())
    }

    /// Portal-local delivery sequence (neither durable nor cluster-global).
    #[napi(getter)]
    pub fn sequence(&self) -> i64 {
        saturating(self.sequence)
    }

    #[napi(getter)]
    pub fn epoch(&self) -> Option<i64> {
        self.epoch.map(saturating)
    }

    #[napi(getter)]
    pub fn checkpoint_id(&self) -> Option<i64> {
        self.checkpoint_id.map(saturating)
    }

    #[napi(getter)]
    pub fn through_sequence(&self) -> Option<i64> {
        self.through_sequence.map(saturating)
    }
}

fn saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Spawn the portal reader: forwards owned frames until the channel closes,
/// the token fires, or a terminal frame arrives. Always closes the portal.
fn spawn_portal_reader(
    mut portal: SubscriptionPortal,
    tx: mpsc::Sender<FrameMsg>,
    token: CancellationToken,
) {
    spawn(async move {
        loop {
            let frame = tokio::select! {
                frame = portal.next_frame() => frame,
                () = token.cancelled() => None,
            };
            let Some(frame) = frame else {
                let _ = tx.send(FrameMsg::Closed).await;
                break;
            };
            let (msg, terminal) = match frame {
                // Dropping the frame releases the shared-log lease; the
                // extracted batch owns its buffers independently.
                PortalFrame::Batch {
                    batch, sequence, ..
                } => (FrameMsg::Data { batch, sequence }, false),
                PortalFrame::Barrier {
                    sequence,
                    epoch,
                    checkpoint_id,
                    through_sequence,
                } => (
                    FrameMsg::Barrier {
                        sequence,
                        epoch,
                        checkpoint_id,
                        through_sequence,
                    },
                    false,
                ),
                PortalFrame::Lagged(skipped) => (FrameMsg::Lagged(skipped), true),
                PortalFrame::Error { message } => (FrameMsg::Failed(message), true),
            };
            if tx.send(msg).await.is_err() || terminal {
                break;
            }
        }
        portal.close();
    });
}

/// Shared internals for the pull and push subscription fronts.
struct SubscriptionCore {
    schema: SchemaRef,
    rx: AsyncMutex<mpsc::Receiver<FrameMsg>>,
    token: CancellationToken,
    active: AtomicBool,
}

impl SubscriptionCore {
    async fn open(
        db: &laminar_db::LaminarDB,
        name: &str,
        filter: Option<&str>,
        from_epoch: Option<u64>,
    ) -> Result<Self> {
        let start = match from_epoch {
            Some(epoch) => laminar_db::subscription::SubscribeStart::AsOfEpoch(epoch),
            None => laminar_db::subscription::SubscribeStart::Tail,
        };
        let portal = db
            .open_subscription(name, filter, start)
            .await
            .map_err(map_db_error)?;
        let schema = portal.schema();
        let (tx, rx) = mpsc::channel(128);
        let token = CancellationToken::new();
        spawn_portal_reader(portal, tx, token.clone());
        Ok(Self {
            schema,
            rx: AsyncMutex::new(rx),
            token,
            active: AtomicBool::new(true),
        })
    }
}

/// A pull-based framed subscription to a named stream or materialized view.
///
/// `nextFrame()` resolves one frame at a time (natural backpressure); `null`
/// means end-of-stream; terminal failures reject once (`LAMINAR_502` for
/// lag, `LAMINAR_500` otherwise) and end the subscription. `cancel()` is
/// idempotent and wakes a pending `nextFrame` with `null`.
#[napi]
pub struct Subscription {
    core: SubscriptionCore,
}

impl Subscription {
    pub async fn open(
        db: &laminar_db::LaminarDB,
        name: &str,
        filter: Option<&str>,
        from_epoch: Option<u64>,
    ) -> Result<Self> {
        Ok(Self {
            core: SubscriptionCore::open(db, name, filter, from_epoch).await?,
        })
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Lifecycle backstop: an abandoned subscription must not pin the
        // portal (or the engine) forever.
        self.core.token.cancel();
    }
}

#[napi]
impl Subscription {
    /// Output schema of the subscribed object.
    #[napi]
    pub fn schema(&self) -> Vec<FieldInfo> {
        field_infos(&self.core.schema)
    }

    /// Wait for the next frame; `null` on end-of-stream or after `cancel()`.
    /// Terminal failures reject with `LAMINAR_502` (lag) or `LAMINAR_500`.
    #[napi]
    pub async fn next_frame(&self) -> Result<Option<SubscriptionFrame>> {
        let mut receiver = self.core.rx.lock().await;
        let Some(msg) = receiver.recv().await else {
            self.deactivate();
            return Ok(None);
        };
        match msg {
            FrameMsg::Lagged(skipped) => {
                self.deactivate();
                Err(coded_error(
                    502,
                    &format!("subscription fell behind by {skipped} entries"),
                ))
            }
            FrameMsg::Failed(message) => {
                self.deactivate();
                Err(coded_error(500, &message))
            }
            FrameMsg::Closed => {
                self.deactivate();
                Ok(None)
            }
            msg => Ok(SubscriptionFrame::from_msg(msg)),
        }
    }

    #[napi]
    pub fn is_active(&self) -> bool {
        self.core.active.load(Ordering::Acquire) && !self.core.token.is_cancelled()
    }

    /// Stop the subscription (idempotent); a pending `nextFrame` resolves
    /// `null`.
    #[napi]
    pub fn cancel(&self) {
        self.deactivate();
        self.core.token.cancel();
    }

    fn deactivate(&self) {
        self.core.active.store(false, Ordering::Release);
    }
}

/// The error payload for push-subscription `onError` callbacks. An object
/// rather than a spread pair: threadsafe calls deliver exactly one JS
/// argument per message.
#[napi(object)]
pub struct CallbackError {
    /// Core error code (e.g. 500, 502).
    pub code: i32,
    /// Message including the `[LAMINAR_<code>]` prefix.
    pub message: String,
}

pub type DataCallback = ThreadsafeFunction<
    SubscriptionFrame,
    napi::bindgen_prelude::Promise<()>,
    SubscriptionFrame,
    napi::Status,
    false,
    true,
    0,
>;
pub type ErrorCallback =
    ThreadsafeFunction<CallbackError, (), CallbackError, napi::Status, false, true, 0>;
pub type CloseCallback = ThreadsafeFunction<(), (), (), napi::Status, false, true, 0>;

/// A push-based subscription: frames are delivered to `onData` one at a time
/// (each delivery is awaited, so a slow consumer applies backpressure
/// instead of growing a queue). `close()` stops delivery and resolves only
/// after the reader has stopped — no callbacks fire after it returns.
#[napi]
pub struct PushSubscription {
    token: CancellationToken,
    done: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
    active: AtomicBool,
}

impl Drop for PushSubscription {
    fn drop(&mut self) {
        // Lifecycle backstop (see `Subscription::drop`). The task holds only
        // weak callback refs, so nothing pins the event loop.
        self.token.cancel();
    }
}

#[napi]
impl PushSubscription {
    #[napi]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Stop delivery and wait for the reader to finish. Idempotent; the
    /// first caller performs the wait.
    #[napi]
    pub async fn close(&self) -> Result<()> {
        self.token.cancel();
        self.active.store(false, Ordering::Release);
        let handle = { self.done.lock().await.take() };
        if let Some(handle) = handle {
            handle.await.map_err(|error| {
                coded_error(900, &format!("subscription reader failed: {error}"))
            })?;
        }
        Ok(())
    }
}

/// Open a push subscription: the weak threadsafe callbacks arrive already
/// built by napi argument conversion; this factory drives the portal reader
/// on a spawned task. Open errors and terminal frames surface through
/// `onError(code, message)` followed by `onClose` — it returns immediately
/// with the handle.
///
/// WHY weak callbacks: a strong ref would keep the Node event loop alive for
/// a stream nobody consumes — a process-lifetime bug, not a convenience.
pub fn spawn_push_subscription(
    db: std::sync::Arc<laminar_db::LaminarDB>,
    name: String,
    filter: Option<String>,
    from_epoch: Option<u64>,
    on_data: DataCallback,
    on_error: ErrorCallback,
    on_close: CloseCallback,
) -> Result<PushSubscription> {
    let token = CancellationToken::new();
    let task_token = token.clone();
    let done = spawn(async move {
        let core = SubscriptionCore::open(&db, &name, filter.as_deref(), from_epoch).await;
        let core = match core {
            Ok(core) => core,
            Err(error) => {
                emit_error(&on_error, &error).await;
                let _ = on_close.call_async(()).await;
                return;
            }
        };
        push_loop(core, task_token, on_data, on_error, on_close).await;
    });

    Ok(PushSubscription {
        token,
        done: AsyncMutex::new(Some(done)),
        active: AtomicBool::new(true),
    })
}

/// Delivery contract: `onData` returns a promise; the reader awaits its
/// SETTLEMENT before pulling the next frame — a slow async handler
/// backpressures the stream. The TypeScript facade normalizes sync handlers
/// to always-returning promises; native-seam callbacks should return a
/// promise (void returners only work through the facade).
async fn push_loop(
    core: SubscriptionCore,
    token: CancellationToken,
    on_data: DataCallback,
    on_error: ErrorCallback,
    on_close: CloseCallback,
) {
    loop {
        let msg = {
            let mut receiver = core.rx.lock().await;
            let msg = tokio::select! {
                msg = receiver.recv() => msg,
                () = token.cancelled() => None,
            };
            msg
        };
        let Some(msg) = msg else {
            break;
        };
        let terminal = matches!(msg, FrameMsg::Lagged(_) | FrameMsg::Failed(_));
        match msg {
            FrameMsg::Data { .. } | FrameMsg::Barrier { .. } => {
                let Some(frame) = SubscriptionFrame::from_msg(msg) else {
                    break;
                };
                match on_data.call_async(frame).await {
                    // Consumer gone (weak ref dropped): stop quietly.
                    Err(_) => break,
                    Ok(promise) => {
                        if let Err(error) = promise.await {
                            // A rejecting handler must not be spammed: report once, stop.
                            emit_error(
                                &on_error,
                                &coded_error(900, &format!("onData handler rejected: {error}")),
                            )
                            .await;
                            break;
                        }
                    }
                }
            }
            FrameMsg::Lagged(skipped) => {
                emit_error(
                    &on_error,
                    &coded_error(
                        502,
                        &format!("subscription fell behind by {skipped} entries"),
                    ),
                )
                .await;
            }
            FrameMsg::Failed(message) => {
                emit_error(&on_error, &coded_error(500, &message)).await;
            }
            FrameMsg::Closed => {}
        }
        if terminal {
            break;
        }
    }
    core.token.cancel();
    let _ = on_close.call_async(()).await;
}

async fn emit_error(on_error: &ErrorCallback, error: &Error) {
    let message = error.reason.clone();
    let code = message
        .strip_prefix("[LAMINAR_")
        .and_then(|rest| rest.split(']').next())
        .and_then(|digits| digits.parse::<i32>().ok())
        .unwrap_or(500);
    let _ = on_error.call_async(CallbackError { code, message }).await;
}
