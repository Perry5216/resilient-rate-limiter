import { EventEmitter } from 'node:events';
import { hrtimeMs } from './clock.js';

export const CircuitState = Object.freeze({
  CLOSED: 'CLOSED',
  OPEN: 'OPEN',
  HALF_OPEN: 'HALF_OPEN'
});

/**
 * CircuitBreaker — three-state machine (CLOSED/OPEN/HALF_OPEN) with rolling failure window.
 *
 * Implements transient fault isolation with fast-fail, controlled recovery probes,
 * fallback execution, and full EventEmitter telemetry.
 */
export class CircuitBreaker extends EventEmitter {
  constructor({
    failureThresholdPct = 50,
    minimumRequests = 5,
    recoveryTimeoutMs = 500,
    halfOpenSuccessThreshold = 2,
    nowFn = hrtimeMs
  } = {}) {
    super();

    if (!Number.isFinite(failureThresholdPct) || failureThresholdPct <= 0 || failureThresholdPct > 100) {
      throw new Error('failureThresholdPct must be between 1 and 100');
    }
    if (!Number.isInteger(minimumRequests) || minimumRequests <= 0) {
      throw new Error('minimumRequests must be a positive integer');
    }
    if (!Number.isFinite(recoveryTimeoutMs) || recoveryTimeoutMs <= 0) {
      throw new Error('recoveryTimeoutMs must be a positive number');
    }
    if (!Number.isInteger(halfOpenSuccessThreshold) || halfOpenSuccessThreshold <= 0) {
      throw new Error('halfOpenSuccessThreshold must be a positive integer');
    }

    this.failureThresholdPct = failureThresholdPct;
    this.minimumRequests = minimumRequests;
    this.recoveryTimeoutMs = recoveryTimeoutMs;
    this.halfOpenSuccessThreshold = halfOpenSuccessThreshold;
    this.nowFn = nowFn;

    this.state = CircuitState.CLOSED;
    this.lastStateChangeMs = this.nowFn();
    this.consecutiveHalfOpenSuccesses = 0;
    this.activeHalfOpenProbes = 0;

    /** @type {boolean[]} Ring buffer of last outcomes (true = success, false = failure) */
    this.ringBuffer = [];
  }

  _nowMs() {
    return this.nowFn();
  }

  getState() {
    this._checkRecoveryTimeout();
    return this.state;
  }

  _transitionTo(newState) {
    if (this.state === newState) return;
    const oldState = this.state;
    this.state = newState;
    const now = this._nowMs();
    this.lastStateChangeMs = now;

    if (newState === CircuitState.HALF_OPEN) {
      this.consecutiveHalfOpenSuccesses = 0;
      this.activeHalfOpenProbes = 0;
    } else if (newState === CircuitState.CLOSED) {
      this.ringBuffer = [];
      this.consecutiveHalfOpenSuccesses = 0;
      this.activeHalfOpenProbes = 0;
    } else if (newState === CircuitState.OPEN) {
      this.consecutiveHalfOpenSuccesses = 0;
      this.activeHalfOpenProbes = 0;
      this.emit('trip', { timestamp: now });
    }

    this.emit('stateChange', { from: oldState, to: newState, timestamp: now });
  }

  _checkRecoveryTimeout() {
    if (this.state === CircuitState.OPEN) {
      const now = this._nowMs();
      if (now - this.lastStateChangeMs >= this.recoveryTimeoutMs) {
        this._transitionTo(CircuitState.HALF_OPEN);
      }
    }
  }

  _recordOutcome(isSuccess) {
    this.ringBuffer.push(isSuccess);
    if (this.ringBuffer.length > this.minimumRequests) {
      this.ringBuffer.shift();
    }
  }

  _computeFailureStats() {
    const totalRecorded = this.ringBuffer.length;
    const failures = this.ringBuffer.filter((outcome) => !outcome).length;
    const successes = totalRecorded - failures;
    const failurePct = totalRecorded > 0 ? (failures / totalRecorded) * 100 : 0;
    return { totalRecorded, successes, failures, failurePct };
  }

  _shouldTrip() {
    if (this.ringBuffer.length < this.minimumRequests) {
      return false;
    }
    const { failurePct } = this._computeFailureStats();
    return failurePct >= this.failureThresholdPct;
  }

  /**
   * Execute an operation protected by the circuit breaker.
   *
   * @param {() => Promise<unknown> | unknown} actionFn
   * @param {((error: Error) => Promise<unknown> | unknown) | null} fallbackFn
   * @returns {Promise<unknown>}
   */
  async execute(actionFn, fallbackFn = null) {
    if (typeof actionFn !== 'function') {
      throw new TypeError('actionFn must be a callable function');
    }

    this._checkRecoveryTimeout();

    if (this.state === CircuitState.OPEN) {
      const openError = new Error('Circuit breaker is OPEN');
      if (typeof fallbackFn === 'function') {
        return await fallbackFn(openError);
      }
      throw openError;
    }

    if (this.state === CircuitState.HALF_OPEN) {
      this.activeHalfOpenProbes++;
    }

    try {
      const result = await actionFn();
      this._handleSuccess();
      return result;
    } catch (err) {
      const errorObj = err instanceof Error ? err : new Error(String(err));
      this._handleFailure(errorObj);
      if (typeof fallbackFn === 'function') {
        return await fallbackFn(errorObj);
      }
      throw errorObj;
    } finally {
      if (this.state === CircuitState.HALF_OPEN && this.activeHalfOpenProbes > 0) {
        this.activeHalfOpenProbes--;
      }
    }
  }

  _handleSuccess() {
    const now = this._nowMs();
    this.emit('success', { timestamp: now, state: this.state });

    if (this.state === CircuitState.HALF_OPEN) {
      this.consecutiveHalfOpenSuccesses++;
      if (this.consecutiveHalfOpenSuccesses >= this.halfOpenSuccessThreshold) {
        this._transitionTo(CircuitState.CLOSED);
      }
    } else if (this.state === CircuitState.CLOSED) {
      this._recordOutcome(true);
    }
  }

  _handleFailure(error) {
    const now = this._nowMs();
    this.emit('failure', { timestamp: now, state: this.state, error });

    if (this.state === CircuitState.HALF_OPEN) {
      this._transitionTo(CircuitState.OPEN);
    } else if (this.state === CircuitState.CLOSED) {
      this._recordOutcome(false);
      if (this._shouldTrip()) {
        this._transitionTo(CircuitState.OPEN);
      }
    }
  }

  getMetrics() {
    const { totalRecorded, successes, failures, failurePct } = this._computeFailureStats();
    return {
      state: this.state,
      totalRecorded,
      successes,
      failures,
      failurePct
    };
  }
}
