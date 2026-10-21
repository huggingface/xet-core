//! Hedges the download that holds back an in-order write.
//!
//! Terms are written in order, so when the oldest unfinished download is slow, the ones after it finish and wait
//! in the download buffer until it fills. A connection that keeps receiving a trickle of data never hits the read
//! timeout, so without help the whole download stalls on it.

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::data_writer::DataFuture;
use super::reconstruction_terms::XorbBlock;

/// The oldest unfinished download is hedged once it takes this many times longer than the downloads that
/// finished after it took for the same number of bytes.
const HEDGE_SLOWDOWN_FACTOR: f64 = 3.0;

/// No download is hedged before this, so request latency on small blocks does not trigger it.
const MIN_HEDGE_DELAY: Duration = Duration::from_secs(2);

struct TrackedTerm {
    size: u64,
    started: Instant,
    /// When its data was ready; `None` while it downloads.
    finished: Option<Instant>,
    /// False when the block was already downloaded when the term was scheduled.
    timed: bool,
    hedged: bool,
    xorb_block: Arc<XorbBlock>,
}

/// Uncompressed size of everything a block download fetches, which several terms can share.
fn block_size(block: &XorbBlock) -> u64 {
    match block.uncompressed_size_if_known {
        Some(size) => size as u64,
        None => block.references.iter().map(|r| r.uncompressed_size as u64).sum(),
    }
}

#[derive(Debug, PartialEq)]
enum HedgeDecision {
    Hedge,
    RecheckAt(Instant),
    Wait,
}

/// Decides whether to hedge the block of the first unfinished term in `terms`, which are in write order.
///
/// It is hedged when the terms finished after it hold more bytes than it does, so the write is held back by it
/// rather than by the network, and its block has taken [`HEDGE_SLOWDOWN_FACTOR`] times longer than the blocks
/// downloaded after it would take for its size, and at least [`MIN_HEDGE_DELAY`]. Only blocks downloaded while
/// it was in progress count, so a link that is slow for every request does not trigger it.
fn decide(terms: &VecDeque<TrackedTerm>, now: Instant) -> HedgeDecision {
    let Some(head_index) = terms.iter().position(|t| t.finished.is_none()) else {
        return HedgeDecision::Wait;
    };
    let head = &terms[head_index];
    if head.hedged {
        return HedgeDecision::Wait;
    }

    let finished_after: Vec<_> = terms.iter().skip(head_index + 1).filter(|t| t.finished.is_some()).collect();
    let ready_bytes: u64 = finished_after.iter().map(|t| t.size).sum();

    // Time each block once, from its first term's start, leaving out the head's block and cached data.
    let (mut timed_bytes, mut timed_secs) = (0, 0.);
    let mut seen: Vec<*const XorbBlock> = vec![];
    for term in finished_after.iter().filter(|t| t.timed) {
        let block = &term.xorb_block;
        if Arc::ptr_eq(block, &head.xorb_block)
            || block.served_from_cache.load(Ordering::Relaxed)
            || seen.contains(&Arc::as_ptr(block))
        {
            continue;
        }
        seen.push(Arc::as_ptr(block));
        if let Some(finished) = term.finished {
            timed_bytes += block_size(block);
            timed_secs += finished.duration_since(term.started).as_secs_f64();
        }
    }
    if ready_bytes <= head.size || timed_bytes == 0 {
        return HedgeDecision::Wait;
    }

    let expected_secs = block_size(&head.xorb_block) as f64 * timed_secs / timed_bytes as f64;
    let delay = Duration::from_secs_f64(expected_secs * HEDGE_SLOWDOWN_FACTOR).max(MIN_HEDGE_DELAY);
    let deadline = head.started + delay;
    if now >= deadline {
        HedgeDecision::Hedge
    } else {
        HedgeDecision::RecheckAt(deadline)
    }
}

#[derive(Default)]
struct TrackedTerms {
    /// Terms in write order. Finished terms at the front are dropped as they no longer matter.
    terms: VecDeque<TrackedTerm>,
    /// Index in write order of `terms[0]`.
    first_index: u64,
}

#[derive(Default)]
struct HedgerState {
    tracked: Mutex<TrackedTerms>,
    changed: Notify,
}

impl HedgerState {
    fn lock(&self) -> std::sync::MutexGuard<'_, TrackedTerms> {
        self.tracked.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn finish(&self, index: u64) {
        let mut tracked = self.lock();
        if let Some(i) = index.checked_sub(tracked.first_index)
            && let Some(term) = tracked.terms.get_mut(i as usize)
        {
            term.finished = Some(Instant::now());
        }
        while tracked.terms.front().is_some_and(|t| t.finished.is_some()) {
            tracked.terms.pop_front();
            tracked.first_index += 1;
        }
        drop(tracked);
        self.changed.notify_one();
    }

    async fn monitor(self: Arc<Self>) {
        loop {
            let decision = {
                let mut tracked = self.lock();
                let decision = decide(&tracked.terms, Instant::now());
                if decision == HedgeDecision::Hedge
                    && let Some(head) = tracked.terms.iter_mut().find(|t| t.finished.is_none())
                {
                    head.hedged = true;
                    tracing::info!(
                        xorb_hash = %head.xorb_block.xorb_hash,
                        size = head.size,
                        "Download holding back the write is slow, starting a second download"
                    );
                    head.xorb_block.request_hedge();
                }
                decision
            };
            match decision {
                HedgeDecision::Hedge => {},
                HedgeDecision::RecheckAt(deadline) => {
                    tokio::select! {
                        _ = tokio::time::sleep_until(deadline) => {},
                        _ = self.changed.notified() => {},
                    }
                },
                HedgeDecision::Wait => self.changed.notified().await,
            }
        }
    }
}

/// Tracks the terms of one in-order reconstruction and hedges the download that holds back the write.
pub(crate) struct HeadOfLineHedger {
    state: Arc<HedgerState>,
    next_index: u64,
    monitor: JoinHandle<()>,
}

impl HeadOfLineHedger {
    pub(crate) fn new() -> Self {
        let state = Arc::new(HedgerState::default());
        let monitor = tokio::spawn(state.clone().monitor());
        Self {
            state,
            next_index: 0,
            monitor,
        }
    }

    /// Tracks the next term in write order, of `size` bytes from `xorb_block`, returning `data_future` wrapped to
    /// record when it finishes.
    pub(crate) fn track(&mut self, size: u64, xorb_block: Arc<XorbBlock>, data_future: DataFuture) -> DataFuture {
        let index = self.next_index;
        self.next_index += 1;
        self.state.lock().terms.push_back(TrackedTerm {
            size,
            started: Instant::now(),
            finished: None,
            timed: xorb_block.data.get().is_none(),
            hedged: false,
            xorb_block,
        });

        let state = self.state.clone();
        Box::pin(async move {
            let result = data_future.await;
            state.finish(index);
            result
        })
    }
}

impl Drop for HeadOfLineHedger {
    fn drop(&mut self) {
        self.monitor.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(size: u64) -> Arc<XorbBlock> {
        Arc::new(XorbBlock {
            xorb_hash: Default::default(),
            chunk_ranges: vec![],
            xorb_block_index: 0,
            references: vec![],
            uncompressed_size_if_known: Some(size as usize),
            data: Default::default(),
            hedge: Default::default(),
            served_from_cache: Default::default(),
        })
    }

    fn term(xorb_block: Arc<XorbBlock>, size: u64, started: Instant, finished: Option<Instant>) -> TrackedTerm {
        TrackedTerm {
            size,
            started,
            finished,
            timed: true,
            hedged: false,
            xorb_block,
        }
    }

    /// A head of `size` bytes started at `t0`, with `count` blocks of `size` bytes behind it that each took 1s.
    fn slow_head(t0: Instant, size: u64, count: usize) -> VecDeque<TrackedTerm> {
        let mut terms: VecDeque<_> = [term(block(size), size, t0, None)].into_iter().collect();
        for _ in 0..count {
            terms.push_back(term(block(size), size, t0, Some(t0 + Duration::from_secs(1))));
        }
        terms
    }

    #[test]
    fn test_slow_head_is_hedged_once() {
        let t0 = Instant::now();
        let mut terms = slow_head(t0, 100, 4);
        // 100 bytes take 1s, so the head is hedged at 3s.
        assert_eq!(decide(&terms, t0 + Duration::from_secs(2)), HedgeDecision::RecheckAt(t0 + Duration::from_secs(3)));
        assert_eq!(decide(&terms, t0 + Duration::from_secs(3)), HedgeDecision::Hedge);

        terms[0].hedged = true;
        assert_eq!(decide(&terms, t0 + Duration::from_secs(10)), HedgeDecision::Wait);
    }

    #[test]
    fn test_head_is_not_hedged_without_more_ready_behind() {
        let t0 = Instant::now();
        let terms: VecDeque<_> = [
            term(block(1000), 1000, t0, None),
            term(block(100), 100, t0, Some(t0 + Duration::from_secs(1))),
        ]
        .into_iter()
        .collect();
        assert_eq!(decide(&terms, t0 + Duration::from_secs(1000)), HedgeDecision::Wait);
    }

    #[test]
    fn test_uniformly_slow_link_is_not_hedged_early() {
        // Every block goes at 10 bytes/s, so the 100 byte head should take 10s.
        let t0 = Instant::now();
        let mut terms: VecDeque<_> = [term(block(100), 100, t0, None)].into_iter().collect();
        for i in 0..20 {
            let started = t0 + Duration::from_secs(i);
            terms.push_back(term(block(10), 10, started, Some(started + Duration::from_secs(1))));
        }
        assert_eq!(
            decide(&terms, t0 + Duration::from_secs(20)),
            HedgeDecision::RecheckAt(t0 + Duration::from_secs(30))
        );
    }

    #[test]
    fn test_small_head_term_waits_for_its_whole_block() {
        // The head term is 10 bytes of a 1000 byte block, behind which blocks go at 100 bytes/s.
        let t0 = Instant::now();
        let shared = block(1000);
        let mut terms: VecDeque<_> = [term(shared.clone(), 10, t0, None), term(shared, 990, t0, None)]
            .into_iter()
            .collect();
        for _ in 0..2 {
            terms.push_back(term(block(100), 100, t0, Some(t0 + Duration::from_secs(1))));
        }
        assert_eq!(decide(&terms, t0 + Duration::from_secs(5)), HedgeDecision::RecheckAt(t0 + Duration::from_secs(30)));
    }

    #[test]
    fn test_cached_blocks_are_not_timed() {
        let t0 = Instant::now();
        let mut terms = slow_head(t0, 100, 2);
        terms[1].xorb_block.served_from_cache.store(true, Ordering::Relaxed);
        terms[2].timed = false;
        assert_eq!(decide(&terms, t0 + Duration::from_secs(1000)), HedgeDecision::Wait);
    }

    #[test]
    fn test_hedge_waits_at_least_the_minimum_delay() {
        // Blocks behind the head finished in 1ms, but the head is not hedged before 2s.
        let t0 = Instant::now();
        let mut terms: VecDeque<_> = [term(block(100), 100, t0, None)].into_iter().collect();
        for _ in 0..4 {
            terms.push_back(term(block(100), 100, t0, Some(t0 + Duration::from_millis(1))));
        }
        assert_eq!(decide(&terms, t0 + Duration::from_millis(500)), HedgeDecision::RecheckAt(t0 + MIN_HEDGE_DELAY));
    }
}
