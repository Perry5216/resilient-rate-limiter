import { hrtimeMs } from './clock.js';

export class AdaptiveConcurrencyLimiter {
  constructor({
    minLimit = 1,
    maxLimit = 20,
    initialLimit = 5,
    smoothingFactor = 0.2,
    backoffFactor = 0.7,
    tolerance = 1.25,
    nowFn = hrtimeMs
  } = {}) {
    if (!Number.isInteger(minLimit) || minLimit <= 0) throw new Error('minLimit must be a positive integer');
    if (!Number.isInteger(maxLimit) || maxLimit < minLimit) throw new Error('maxLimit must be >= minLimit');
    if (!Number.isInteger(initialLimit) || initialLimit < minLimit || initialLimit > maxLimit) {
      throw new Error('initialLimit must be between minLimit and maxLimit');
    }
    if (!Number.isFinite(smoothingFactor) || smoothingFactor <= 0 || smoothingFactor >= 1) {
      throw new Error('smoothingFactor must be between 0 and 1');
    }
    if (!Number.isFinite(backoffFactor) || backoffFactor <= 0 || backoffFactor >= 1) {
      throw new Error('backoffFactor must be between 0 and 1');
    }
    if (!Number.isFinite(tolerance) || tolerance <= 0) {
      throw new Error('tolerance must be a positive number');
    }

    this.minLimit = minLimit;
    this.maxLimit = maxLimit;
    this.initialLimit = initialLimit;
    this.limit = initialLimit;
    this.smoothingFactor = smoothingFactor;
    this.backoffFactor = backoffFactor;
    this.tolerance = tolerance;
    this.nowFn = nowFn;

    this.inFlight = 0;
    this.baseRtt = Infinity;
    this.smoothedRtt = 0;
  }

  _nowMs() {
    return this.nowFn();
  }

  async execute(actionFn, { signal = null } = {}) {
    if (typeof actionFn !== 'function') {
      throw new TypeError('actionFn must be a callable function');
    }

    if (signal?.aborted) {
      throw signal.reason || new Error('The operation was aborted');
    }

    if (this.inFlight >= this.limit) {
      throw new Error('Concurrency limit reached');
    }

    this.inFlight++;
    const startMs = this._nowMs();

    return new Promise((resolve, reject) => {
      let settled = false;

      const cleanup = () => {
        if (settled) return;
        settled = true;
        this.inFlight--;
        if (signal && onAbort) {
          signal.removeEventListener('abort', onAbort);
        }
      };

      const onAbort = () => {
        cleanup();
        reject(signal.reason || new Error('The operation was aborted'));
      };

      if (signal) {
        signal.addEventListener('abort', onAbort, { once: true });
      }

      Promise.resolve()
        .then(() => actionFn())
        .then((res) => {
          if (settled) return;
          const rtt = this._nowMs() - startMs;
          cleanup();
          this._recordRtt(rtt);
          resolve(res);
        })
        .catch((err) => {
          if (settled) return;
          cleanup();
          reject(err);
        });
    });
  }

  _recordRtt(rttMs) {
    if (rttMs < this.baseRtt) {
      this.baseRtt = rttMs;
    }
    if (this.smoothedRtt === 0) {
      this.smoothedRtt = rttMs;
      return; // Baseline established; adaptation starts on subsequent samples
    }

    this.smoothedRtt = (1 - this.smoothingFactor) * this.smoothedRtt + this.smoothingFactor * rttMs;

    const gradient = this.smoothedRtt / this.baseRtt;
    if (gradient <= this.tolerance) {
      this.limit = Math.min(this.maxLimit, this.limit + 1);
    } else {
      this.limit = Math.max(this.minLimit, Math.floor(this.limit * this.backoffFactor));
    }
  }

  getStats() {
    return {
      limit: this.limit,
      inFlight: this.inFlight,
      baseRtt: Number.isFinite(this.baseRtt) ? this.baseRtt : 0,
      smoothedRtt: this.smoothedRtt
    };
  }

  reset() {
    this.limit = this.initialLimit;
    this.inFlight = 0;
    this.baseRtt = Infinity;
    this.smoothedRtt = 0;
  }
}
