use std::sync::atomic::{AtomicU64, Ordering};

use futures::Future;
use risingwave_hummock_trace::ConcurrentId;
use tokio::task::futures::TaskLocalFuture;
use tokio::task_local;

static CONCURRENT_ID: AtomicU64 = AtomicU64::new(0);

task_local! {
    // This is why we need to ignore this rule
    // https://github.com/rust-lang/rust-clippy/issues/9224
    #[allow(clippy::declare_interior_mutable_const)]
    pub static LOCAL_ID: ConcurrentId;
}

pub fn hummock_trace_scope<F: Future>(f: F) -> TaskLocalFuture<ConcurrentId, F> {
    #[cfg(all(not(madsim), hm_trace))]
    {
        let id = CONCURRENT_ID.fetch_add(1, Ordering::Relaxed);
        println!("id scoped {:?}", id);
        LOCAL_ID.scope(Some(id), f)
    }
    #[cfg(any(madsim, not(hm_trace)))]
    f
}

pub fn get_concurrent_id() -> ConcurrentId {
    #[cfg(all(not(madsim), hm_trace))]
    {
        LOCAL_ID.get()
    }
    #[cfg(any(madsim, not(hm_trace)))]
    None
}
