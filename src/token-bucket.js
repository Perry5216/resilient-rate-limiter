import { hrtimeMs } from './clock.js';

/**
 * High-Precision Token Bucket Limiter with Fractional Replenishment.
 */
export class TokenBucketLimiter {
  constructor(options = {}) {
    const {
      capacity = 10,
      refillRate = 5,
      initialTokens = capacity,
      nowFn = hrtimeMs
    } = options;

    if (!Number.isFinite(capacity) || capacity <= 0) {
      throw new RangeError('Capacity must be a positive finite number');
    }
    if (!Number.isFinite(refillRate) || refillRate <= 0) {
      throw new RangeError('Refill rate must be a positive finite number');
    }
    if (!Number.isFinite(initialTokens) || initialTokens < 0) {
      throw new RangeError('Initial tokens must be a non-negative finite number');
    }

    this.capacity = capacity;
    this.refillRate = refillRate;
    this.initialTokens = Math.min(initialTokens, capacity);
    this.nowFn = nowFn;

    this.tokens = this.initialTokens;
    this.lastRefillMs = this.nowFn();
  }

  _accrue() {
    const now = this.nowFn();
    const elapsedMs = Math.max(0, now - this.lastRefillMs);
    if (elapsedMs > 0) {
      const tokensToAdd = (elapsedMs * this.refillRate) / 1000.0;
      this.tokens = Math.min(this.capacity, this.tokens + tokensToAdd);
      this.lastRefillMs = now;
    }
    return this.tokens;
  }

  peek() {
    this._accrue();
    return this.tokens;
  }

  tryConsume(tokens = 1) {
    if (!Number.isFinite(tokens) || tokens <= 0) {
      throw new TypeError('Tokens must be a positive finite number');
    }
    this._accrue();

    if (this.tokens >= tokens) {
      this.tokens -= tokens;
      return {
        allowed: true,
        remaining: this.tokens,
        retryAfterMs: 0
      };
    }

    const missingTokens = tokens - this.tokens;
    const retryAfterMs = Math.ceil((missingTokens / this.refillRate) * 1000.0);

    return {
      allowed: false,
      remaining: this.tokens,
      retryAfterMs
    };
  }

  reset() {
    this.tokens = this.initialTokens;
    this.lastRefillMs = this.nowFn();
  }
}
