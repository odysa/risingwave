use std::sync::atomic::{AtomicU64, Ordering};

use futures::Future;
#[cfg(all(not(madsim), feature = "hm-trace"))]
use tokio::task::futures::TaskLocalFuture;
use tokio::task_local;

static CONCURRENT_ID: AtomicU64 = AtomicU64::new(0);

type ConcurrentId = u64;

task_local! {
    // This is why we need to ignore this rule
    // https://github.com/rust-lang/rust-clippy/issues/9224
    #[allow(clippy::declare_interior_mutable_const)]
    pub static LOCAL_ID: ConcurrentId;
}

#[cfg(all(not(madsim), feature = "hm-trace"))]
pub fn hummock_trace_scope<F: Future>(f: F) -> TaskLocalFuture<ConcurrentId, F> {
    let id = CONCURRENT_ID.fetch_add(1, Ordering::Relaxed);
    LOCAL_ID.scope(id, f)
}

#[cfg(any(madsim, not(feature = "hm-trace")))]
pub fn hummock_trace_scope<F: Future>(f: F) -> F {
    f
}

pub fn get_concurrent_id() -> ConcurrentId {
    #[cfg(all(not(madsim), feature = "hm-trace"))]
    {
        LOCAL_ID.get()
    }
    #[cfg(any(madsim, not(feature = "hm-trace")))]
    CONCURRENT_ID.fetch_add(1, Ordering::Relaxed)
}
