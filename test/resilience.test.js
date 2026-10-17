import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SlidingWindowRateLimiter, CircuitBreaker, CircuitState } from '../src/index.js';

describe('SlidingWindowRateLimiter', () => {
  it('permits bursts up to limit within the window', () => {
    let currentTime = 1000.0;
    const limiter = new SlidingWindowRateLimiter({
      limit: 5,
      windowMs: 1000,
      granularBuckets: 10,
      nowFn: () => currentTime
    });

    for (let i = 0; i < 5; i++) {
      const res = limiter.tryAcquire(1);
      assert.equal(res.allowed, true, `Token ${i + 1} should be allowed`);
      assert.equal(res.remaining, 4 - i, `Remaining should be ${4 - i}`);
    }

    const over = limiter.tryAcquire(1);
    assert.equal(over.allowed, false, 'Over-limit token should be rejected');
    assert.equal(over.remaining, 0, 'Remaining should be 0');
    assert.ok(over.resetMs > 0, 'resetMs should be positive');
  });

  it('slides window and replenishes capacity as sub-buckets expire', () => {
    let currentTime = 1000.0;
    const limiter = new SlidingWindowRateLimiter({
      limit: 10,
      windowMs: 1000,
      granularBuckets: 10, // bucket width = 100ms
      nowFn: () => currentTime
    });

    // Consume 6 tokens at t=1000
    assert.equal(limiter.tryAcquire(6).allowed, true);

    // Advance 250ms (t=1250) and consume 4 tokens (exhausts limit 10)
    currentTime += 250;
    assert.equal(limiter.tryAcquire(4).allowed, true);
    assert.equal(limiter.tryAcquire(1).allowed, false);

    // Advance past t=2050 (1050ms after first burst): the first 6 tokens expire, but the 4 tokens at t=1250 are still inside window
    currentTime = 2050;
    const stats = limiter.getStats();
    assert.equal(stats.currentLoad, 4, 'Only 4 tokens should remain in window');
    assert.equal(stats.remaining, 6, '6 tokens should be replenished');

    // Should now allow 6 tokens
    assert.equal(limiter.tryAcquire(6).allowed, true);
    assert.equal(limiter.tryAcquire(1).allowed, false);
  });

  it('handles async acquire timeout rejection', async () => {
    let currentTime = 1000.0;
    const limiter = new SlidingWindowRateLimiter({
      limit: 1,
      windowMs: 1000,
      nowFn: () => currentTime
    });

    assert.equal(limiter.tryAcquire(1).allowed, true);

    // Attempting acquire with 100ms timeout
    const acquirePromise = limiter.acquire(1, 100);

    // Advance time past deadline
    currentTime += 150;
    limiter._drainQueue();

    await assert.rejects(acquirePromise, {
      name: 'Error',
      message: 'Rate limit acquire timed out'
    });
  });
});

describe('CircuitBreaker', () => {
  it('remains CLOSED during healthy executions', async () => {
    let currentTime = 1000.0;
    const breaker = new CircuitBreaker({
      failureThresholdPct: 50,
      minimumRequests: 4,
      recoveryTimeoutMs: 500,
      nowFn: () => currentTime
    });

    const result = await breaker.execute(async () => 'OK');
    assert.equal(result, 'OK');
    assert.equal(breaker.getState(), CircuitState.CLOSED);
  });

  it('trips to OPEN when failure threshold is breached', async () => {
    let currentTime = 1000.0;
    const breaker = new CircuitBreaker({
      failureThresholdPct: 50,
      minimumRequests: 4,
      recoveryTimeoutMs: 500,
      nowFn: () => currentTime
    });

    let tripEventFired = false;
    let stateChangeEvent = null;

    breaker.on('trip', () => { tripEventFired = true; });
    breaker.on('stateChange', (ev) => { stateChangeEvent = ev; });

    // 2 successes
    await breaker.execute(async () => 'OK');
    await breaker.execute(async () => 'OK');

    // 2 failures (2 out of 4 = 50% failures, meets failureThresholdPct)
    await assert.rejects(breaker.execute(async () => { throw new Error('Fault 1'); }));
    await assert.rejects(breaker.execute(async () => { throw new Error('Fault 2'); }));

    assert.equal(breaker.getState(), CircuitState.OPEN);
    assert.equal(tripEventFired, true, 'Trip event should fire');
    assert.deepEqual(stateChangeEvent, { from: CircuitState.CLOSED, to: CircuitState.OPEN, timestamp: 1000 });
  });

  it('fast-fails while OPEN and invokes fallback if provided', async () => {
    let currentTime = 1000.0;
    const breaker = new CircuitBreaker({
      failureThresholdPct: 50,
      minimumRequests: 2,
      recoveryTimeoutMs: 1000,
      nowFn: () => currentTime
    });

    await assert.rejects(breaker.execute(async () => { throw new Error('Err'); }));
    await assert.rejects(breaker.execute(async () => { throw new Error('Err'); }));
    assert.equal(breaker.getState(), CircuitState.OPEN);

    // Call without fallback throws Circuit breaker is OPEN
    let actionCalled = false;
    await assert.rejects(
      breaker.execute(async () => { actionCalled = true; }),
      { name: 'Error', message: 'Circuit breaker is OPEN' }
    );
    assert.equal(actionCalled, false, 'Action must NOT be called when OPEN');

    // Call with fallback
    const fallbackResult = await breaker.execute(
      async () => { actionCalled = true; },
      async (err) => `FALLBACK_${err.message}`
    );
    assert.equal(fallbackResult, 'FALLBACK_Circuit breaker is OPEN');
  });

  it('transitions to HALF_OPEN after recovery timeout and re-closes on consecutive successes', async () => {
    let currentTime = 1000.0;
    const breaker = new CircuitBreaker({
      failureThresholdPct: 50,
      minimumRequests: 2,
      recoveryTimeoutMs: 500,
      halfOpenSuccessThreshold: 2,
      nowFn: () => currentTime
    });

    await assert.rejects(breaker.execute(async () => { throw new Error('Fail'); }));
    await assert.rejects(breaker.execute(async () => { throw new Error('Fail'); }));
    assert.equal(breaker.getState(), CircuitState.OPEN);

    // Advance past recovery timeout
    currentTime += 550;
    assert.equal(breaker.getState(), CircuitState.HALF_OPEN);

    // First recovery probe
    const p1 = await breaker.execute(async () => 'PROBE_1');
    assert.equal(p1, 'PROBE_1');
    assert.equal(breaker.getState(), CircuitState.HALF_OPEN);

    // Second recovery probe (reaches threshold of 2)
    const p2 = await breaker.execute(async () => 'PROBE_2');
    assert.equal(p2, 'PROBE_2');
    assert.equal(breaker.getState(), CircuitState.CLOSED, 'Should be CLOSED after consecutive probe successes');
  });

  it('immediately trips back to OPEN if a probe fails in HALF_OPEN', async () => {
    let currentTime = 1000.0;
    const breaker = new CircuitBreaker({
      failureThresholdPct: 50,
      minimumRequests: 2,
      recoveryTimeoutMs: 500,
      halfOpenSuccessThreshold: 2,
      nowFn: () => currentTime
    });

    await assert.rejects(breaker.execute(async () => { throw new Error('Fail'); }));
    await assert.rejects(breaker.execute(async () => { throw new Error('Fail'); }));
    assert.equal(breaker.getState(), CircuitState.OPEN);

    currentTime += 600;
    assert.equal(breaker.getState(), CircuitState.HALF_OPEN);

    // Probe failure
    await assert.rejects(breaker.execute(async () => { throw new Error('Probe failed'); }));
    assert.equal(breaker.getState(), CircuitState.OPEN, 'Must immediately trip to OPEN on probe failure');
  });
});
