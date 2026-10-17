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
    /** @type {Array<{tokens: number, deadlineMs: number, resolve: (v: boolean) => void, reject: (e: Error) => void}>} FIFO waiters */
    this.queue = [];
    /** @type {NodeJS.Timeout | null} */
    this._drainTimer = null;
    this._destroyed = false;
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
    const expiryMs = oldestIndex * this.bucketWidthMs + this.windowMs;
    return Math.max(0, Math.ceil(expiryMs - nowMs));
  }

  /**
   * Immediate capacity decision.
   * @param {number} tokens
   * @returns {{ allowed: boolean, remaining: number, resetMs: number }}
   */
  tryAcquire(tokens = 1) {
    if (this._destroyed) throw new Error('Rate limiter destroyed');
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
   * Schedules next timer-driven queue drain based on earliest capacity opening or deadline.
   */
  _scheduleWakeup(deadlineMs = Infinity) {
    if (this._destroyed || this.queue.length === 0) return;
    if (this._drainTimer) {
      clearTimeout(this._drainTimer);
      this._drainTimer = null;
    }

    const nowMs = this._nowMs();
    const reset = this._resetMs(nowMs, this._load(nowMs));
    const nextEarliestDeadline = this.queue.reduce((min, w) => Math.min(min, w.deadlineMs), deadlineMs);
    const targetTime = Math.min(nextEarliestDeadline, nowMs + (reset > 0 ? reset : this.bucketWidthMs));
    const delayMs = Math.max(1, Math.ceil(targetTime - nowMs));

    this._drainTimer = setTimeout(() => {
      this._drainTimer = null;
      this.drain();
    }, delayMs);

    if (typeof this._drainTimer.unref === 'function') {
      this._drainTimer.unref();
    }
  }

  /**
   * Public queue evaluation: processes FIFO queue against current time.
   */
  drain() {
    if (this._destroyed || this.queue.length === 0) return;
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
      break;
    }

    if (this.queue.length > 0) {
      this._scheduleWakeup();
    }
  }

  /**
   * Queue-and-wait acquisition. Resolves `true` when capacity opens; rejects
   * with Error('Rate limit acquire timed out') if `timeoutMs` expires.
   */
  async acquire(tokens = 1, timeoutMs = 5000) {
    if (this._destroyed) return Promise.reject(new Error('Rate limiter destroyed'));
    if (!Number.isInteger(tokens) || tokens <= 0) throw new Error('tokens must be a positive integer');

    const immediate = this.tryAcquire(tokens);
    if (immediate.allowed) return true;

    const deadlineMs = this._nowMs() + timeoutMs;

    return new Promise((resolve, reject) => {
      this.queue.push({ tokens, deadlineMs, resolve, reject });
      this._scheduleWakeup(deadlineMs);
    });
  }

  getStats() {
    const nowMs = this._nowMs();
    const currentLoad = this._load(nowMs);
    return { currentLoad, remaining: this.limit - currentLoad, limit: this.limit, isDestroyed: this._destroyed };
  }

  /**
   * Release resources and reject pending queue waiters.
   */
  destroy() {
    this._destroyed = true;
    if (this._drainTimer) {
      clearTimeout(this._drainTimer);
      this._drainTimer = null;
    }
    while (this.queue.length > 0) {
      const waiter = this.queue.shift();
      waiter.reject(new Error('Rate limiter destroyed'));
    }
    this.buckets.clear();
  }
}
