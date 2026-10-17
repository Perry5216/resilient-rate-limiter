import { hrtimeMs } from './clock.js';

/**
 * SlidingWindowRateLimiter — sub-bucket sliding window counter (ADR 0001).
 *
 * Memory is bounded at `granularBuckets` entries: each request is registered in
 * the sub-bucket containing its timestamp; buckets older than the window are
 * evicted on every decision. `resetMs` is derived from the oldest live bucket's
 * expiry, not a fixed-window reset.
 *
 * Clock seam (ADR 0002): optional `nowFn` returning fractional milliseconds.
 * Default is `process.hrtime.bigint()`-derived ms (microsecond precision).
 */
export class SlidingWindowRateLimiter {
  constructor({ limit, windowMs, granularBuckets = 10, nowFn = hrtimeMs } = {}) {
    if (!Number.isFinite(limit) || limit <= 0) throw new Error('limit must be a positive number');
    if (!Number.isFinite(windowMs) || windowMs <= 0) throw new Error('windowMs must be a positive number');
    if (!Number.isInteger(granularBuckets) || granularBuckets <= 0) {
      throw new Error('granularBuckets must be a positive integer');
    }
    this.limit = limit;
    this.windowMs = windowMs;
    this.granularBuckets = granularBuckets;
    this.bucketWidthMs = windowMs / granularBuckets;
    this.nowFn = nowFn;

    /** @type {Map<number, number>} bucketIndex -> token count */
    this.buckets = new Map();
    /** @type {Array<{tokens: number, resolve: (v: boolean) => void, reject: (e: Error) => void, deadlineMs: number}>} FIFO waiters */
    this.queue = [];
  }

  _nowMs() {
    return this.nowFn();
  }

  _bucketIndex(nowMs) {
    return Math.floor(nowMs / this.bucketWidthMs);
  }

  /** Evict sub-buckets that have fully fallen out of the rolling window. */
  _evict(nowMs) {
    const oldestAllowedIndex = this._bucketIndex(nowMs - this.windowMs);
    for (const index of this.buckets.keys()) {
      // A bucket is live while its expiry (indexStart + bucketWidth) is still
      // inside the window; buckets at or before the window's left edge are gone.
      if (index <= oldestAllowedIndex) this.buckets.delete(index);
    }
  }

  _load(nowMs) {
    this._evict(nowMs);
    let load = 0;
    for (const count of this.buckets.values()) load += count;
    return load;
  }

  /**
   * Earliest moment capacity can reopen: ms until the oldest live bucket
   * slides out of the window. 0 when capacity is already available.
   */
  _resetMs(nowMs, load) {
    if (load < this.limit) return 0;
    let oldestIndex = Infinity;
    for (const [index, count] of this.buckets) {
      if (count > 0 && index < oldestIndex) oldestIndex = index;
    }
    if (oldestIndex === Infinity) return 0;
    const expiryMs = (oldestIndex + 1) * this.bucketWidthMs;
    return Math.max(0, expiryMs - nowMs);
  }

  /**
   * Immediate decision. Returns { allowed, remaining, resetMs }.
   */
  tryAcquire(tokens = 1) {
    if (!Number.isInteger(tokens) || tokens <= 0) throw new Error('tokens must be a positive integer');
    const nowMs = this._nowMs();
    const load = this._load(nowMs);

    if (load + tokens <= this.limit) {
      const index = this._bucketIndex(nowMs);
      this.buckets.set(index, (this.buckets.get(index) ?? 0) + tokens);
      const newLoad = load + tokens;
      return { allowed: true, remaining: this.limit - newLoad, resetMs: this._resetMs(nowMs, newLoad) };
    }

    return { allowed: false, remaining: this.limit - load, resetMs: this._resetMs(nowMs, load) };
  }

  /**
   * Queue-and-wait acquisition. Resolves `true` when capacity opens; rejects
   * with Error('Rate limit acquire timed out') if `timeoutMs` expires.
   *
   * Production path uses real timers against the injected clock. When the clock
   * is virtual (tests), the queue is drained synchronously at a microtask
   * checkpoint after the clock advances (ADR 0002).
   */
  async acquire(tokens = 1, timeoutMs = 5000) {
    if (!Number.isInteger(tokens) || tokens <= 0) throw new Error('tokens must be a positive integer');

    const immediate = this.tryAcquire(tokens);
    if (immediate.allowed) return true;

    const deadlineMs = this._nowMs() + timeoutMs;
    const waiter = { tokens, deadlineMs, resolve: null, reject: null };

    return new Promise((resolve, reject) => {
      waiter.resolve = resolve;
      waiter.reject = reject;
      this.queue.push(waiter);

      const isVirtualClock = this.nowFn !== hrtimeMs;
      if (isVirtualClock) {
        // Microtask checkpoint: tests advance the virtual clock, then drain.
        queueMicrotask(() => this._drainQueue());
      } else {
        // Real clock: schedule a wake-up at the earlier of bucket expiry and deadline.
        const nowMs = this._nowMs();
        const wakeMs = Math.min(deadlineMs, nowMs + this._resetMs(nowMs, this._load(nowMs)));
        const timer = setTimeout(() => this._drainQueue(), Math.max(0, wakeMs - nowMs));
        if (typeof timer.unref === 'function') timer.unref();
      }
    });
  }

  /** Drain the FIFO queue against the current clock; called at bucket expiry / deadline. */
  _drainQueue() {
    if (this.queue.length === 0) return;
    const nowMs = this._nowMs();

    while (this.queue.length > 0) {
      const waiter = this.queue[0];
      if (nowMs >= waiter.deadlineMs) {
        this.queue.shift();
        waiter.reject(new Error('Rate limit acquire timed out'));
        continue;
      }
      const load = this._load(nowMs);
      if (load + waiter.tokens <= this.limit) {
        this.queue.shift();
        const index = this._bucketIndex(nowMs);
        this.buckets.set(index, (this.buckets.get(index) ?? 0) + waiter.tokens);
        waiter.resolve(true);
        continue;
      }
      break; // head-of-line waiter cannot proceed yet; FIFO order preserved
    }

    // If work remains, schedule the next wake-up (real clocks only).
    if (this.queue.length > 0 && this.nowFn !== hrtimeMs) {
      const after = this._nowMs();
      const head = this.queue[0];
      const wakeMs = Math.min(head.deadlineMs, after + this._resetMs(after, this._load(after)));
      const timer = setTimeout(() => this._drainQueue(), Math.max(0, wakeMs - after));
      if (typeof timer.unref === 'function') timer.unref();
    }
  }

  getStats() {
    const nowMs = this._nowMs();
    const currentLoad = this._load(nowMs);
    return { currentLoad, remaining: this.limit - currentLoad, limit: this.limit };
  }
}
