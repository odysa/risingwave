// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod state_store_metrics;

pub use state_store_metrics::*;
mod monitored_store;
pub use monitored_store::*;
mod hummock_metrics;
pub use hummock_metrics::*;

mod local_metrics;
pub use local_metrics::StoreLocalStatistic;
pub use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;

#[cfg(hm_trace)]
mod traced_store;
use futures::Future;
#[cfg(hm_trace)]
pub use traced_store::*;
#[cfg(all(not(madsim), hm_trace))]
use {
    risingwave_hummock_trace::{ConcurrentId, CONCURRENT_ID, LOCAL_ID},
    tokio::task::futures::TaskLocalFuture,
};

pub trait HummockTraceFuture: Sized + Future {
    #[cfg(any(madsim, not(hm_trace)))]
    fn may_trace_hummock(self) -> Self {
        self
    }
    #[cfg(all(not(madsim), hm_trace))]
    fn may_trace_hummock(self) -> TaskLocalFuture<ConcurrentId, Self> {
        let id = CONCURRENT_ID.next();
        LOCAL_ID.scope(id, self)
    }
}

impl<F: Future> HummockTraceFuture for F {}
