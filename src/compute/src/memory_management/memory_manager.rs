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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use risingwave_batch::task::BatchManager;
use risingwave_common::util::epoch::Epoch;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::task::LocalStreamManager;
use tikv_jemalloc_ctl::{epoch as jemalloc_epoch, stats as jemalloc_stats};
use tracing;

/// When `enable_managed_cache` is set, compute node will launch a [`GlobalMemoryManager`] to limit
/// the memory usage.
pub struct GlobalMemoryManager {
    /// All cached data before the watermark should be evicted.
    watermark_epoch: Arc<AtomicU64>,
    /// Total memory can be allocated by the process.
    total_memory_available_bytes: usize,
    /// Barrier interval.
    barrier_interval_ms: u32,
    metrics: Arc<StreamingMetrics>,
}

pub type GlobalMemoryManagerRef = Arc<GlobalMemoryManager>;

impl GlobalMemoryManager {
    const EVICTION_THRESHOLD_AGGRESSIVE: f64 = 0.9;
    const EVICTION_THRESHOLD_GRACEFUL: f64 = 0.7;

    pub fn new(
        total_memory_available_bytes: usize,
        barrier_interval_ms: u32,
        metrics: Arc<StreamingMetrics>,
    ) -> Arc<Self> {
        // Arbitrarily set a minimal barrier interval in case it is too small,
        // especially when it's 0.
        let barrier_interval_ms = std::cmp::max(barrier_interval_ms, 10);

        Arc::new(Self {
            watermark_epoch: Arc::new(0.into()),
            total_memory_available_bytes,
            barrier_interval_ms,
            metrics,
        })
    }

    pub fn get_watermark_epoch(&self) -> Arc<AtomicU64> {
        self.watermark_epoch.clone()
    }

    fn set_watermark_time_ms(&self, time_ms: u64) {
        let epoch = Epoch::from_physical_time(time_ms).0;
        let watermark_epoch = self.watermark_epoch.as_ref();
        watermark_epoch.store(epoch, Ordering::Relaxed);
    }

    /// Memory manager will get memory usage from batch and streaming, and do some actions.
    /// 1. if batch exceeds, kill running query.
    /// 2. if streaming exceeds, evict cache by watermark.
    pub async fn run(
        self: Arc<Self>,
        _batch_mgr: Arc<BatchManager>,
        _stream_mgr: Arc<LocalStreamManager>,
    ) {
        let mem_threshold_graceful =
            (self.total_memory_available_bytes as f64 * Self::EVICTION_THRESHOLD_GRACEFUL) as usize;
        let mem_threshold_aggressive = (self.total_memory_available_bytes as f64
            * Self::EVICTION_THRESHOLD_AGGRESSIVE) as usize;

        let mut watermark_time_ms = Epoch::physical_now();
        let mut last_total_bytes_used = 0;
        let mut step = 0;

        let jemalloc_epoch_mib = jemalloc_epoch::mib().unwrap();
        let jemalloc_allocated_mib = jemalloc_stats::allocated::mib().unwrap();

        let mut tick_interval =
            tokio::time::interval(Duration::from_millis(self.barrier_interval_ms as u64));

        loop {
            // Wait for a while to check if need eviction.
            tick_interval.tick().await;

            if let Err(e) = jemalloc_epoch_mib.advance() {
                tracing::warn!("Jemalloc epoch advance failed! {:?}", e);
            }

            let cur_total_bytes_used = jemalloc_allocated_mib.read().unwrap_or_else(|e| {
                tracing::warn!("Jemalloc read allocated failed! {:?}", e);
                last_total_bytes_used
            });

            // The strategy works as follow:
            //
            // 1. When the memory usage is below the graceful threshold, we do not evict any caches
            // and reset the step to 0.
            //
            // 2. When the memory usage is between the graceful and aggressive threshold:
            //   - If the last eviction memory usage decrease after last eviction, we set the
            //     eviction step to 1
            //   - or else we set the step to last_step + 1
            //
            // 3. When the memory usage exceeds aggressive threshold:
            //   - If the memory usage decrease after last eviction, we set the eviction step to
            //     last_step
            //   - or else we set the step to last_step * 2

            let last_step = step;
            step = if cur_total_bytes_used < mem_threshold_graceful {
                // Do not evict if the memory usage is lower than `mem_threshold_graceful`
                0
            } else if cur_total_bytes_used < mem_threshold_aggressive {
                // Gracefully evict
                if last_total_bytes_used > cur_total_bytes_used {
                    1
                } else {
                    step + 1
                }
            } else if last_total_bytes_used < cur_total_bytes_used {
                // Aggressively evict
                if step == 0 {
                    2
                } else {
                    step * 2
                }
            } else {
                step
            };

            last_total_bytes_used = cur_total_bytes_used;

            // if watermark_time_ms + self.barrier_interval_ms as u64 * step > now, we do not
            // increase the step, and set the epoch to now time epoch.
            let physical_now = Epoch::physical_now();
            if (physical_now - watermark_time_ms) / (self.barrier_interval_ms as u64) < step {
                step = last_step;
                watermark_time_ms = physical_now;
            } else {
                watermark_time_ms += self.barrier_interval_ms as u64 * step;
            }

            self.metrics
                .lru_current_watermark_time_ms
                .set(watermark_time_ms as i64);
            self.metrics.lru_physical_now_ms.set(physical_now as i64);
            self.metrics.lru_watermark_step.set(step as i64);
            self.metrics.lru_runtime_loop_count.inc();
            self.metrics
                .jemalloc_allocated_bytes
                .set(cur_total_bytes_used as i64);

            self.set_watermark_time_ms(watermark_time_ms);
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_stream::cache::EvictableHashMap;

    #[test]
    fn test_len_after_evict() {
        let target_cap = 114;
        let items_count = 514;
        let mut map = EvictableHashMap::new(target_cap);

        for i in 0..items_count {
            map.put(i, ());
        }
        assert_eq!(map.len(), items_count);

        map.evict_to_target_cap();
        assert_eq!(map.len(), target_cap);

        assert!(map.get(&(items_count - target_cap - 1)).is_none());
        assert!(map.get(&(items_count - target_cap)).is_some());
    }
}
