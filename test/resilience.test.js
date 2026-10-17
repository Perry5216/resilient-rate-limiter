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

  it('handles async acquire resolution via public drain', async () => {
    let currentTime = 1000.0;
    const limiter = new SlidingWindowRateLimiter({
      limit: 1,
      windowMs: 500,
      granularBuckets: 5,
      nowFn: () => currentTime
    });

    assert.equal(limiter.tryAcquire(1).allowed, true);

    // Queue second request
    const acquirePromise = limiter.acquire(1, 1000);

    // Advance time past expiry and call public drain()
    currentTime += 600;
    limiter.drain();

    const acquired = await acquirePromise;
    assert.equal(acquired, true, 'Queued acquire should resolve to true once capacity opens');
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
    limiter.drain();

    await assert.rejects(acquirePromise, {
      name: 'Error',
      message: 'Rate limit acquire timed out'
    });
  });

  it('resolves real-clock acquire automatically with real timer', async () => {
    const limiter = new SlidingWindowRateLimiter({
      limit: 1,
      windowMs: 50,
      granularBuckets: 5
    });

    assert.equal(limiter.tryAcquire(1).allowed, true);

    const start = Date.now();
    const acquired = await limiter.acquire(1, 300);
    const elapsed = Date.now() - start;

    assert.equal(acquired, true);
    assert.ok(elapsed >= 40, `Should take at least ~50ms to open, took ${elapsed}ms`);
  });

  it('accurately computes resetMs as the duration until oldest bucket slides out of window', () => {
    let currentTime = 1000.0;
    const limiter = new SlidingWindowRateLimiter({
      limit: 2,
      windowMs: 1000,
      granularBuckets: 10,
      nowFn: () => currentTime
    });

    // Exhaust limit at t=1000
    limiter.tryAcquire(2);

    // Over-limit attempt at t=1000: oldest bucket is at t=1000, expires at t=2000 -> resetMs = 1000
    const over1 = limiter.tryAcquire(1);
    assert.equal(over1.allowed, false);
    assert.equal(over1.resetMs, 1000, 'resetMs at t=1000 should be 1000ms');

    // Advance 400ms to t=1400: resetMs should be 600ms
    currentTime = 1400;
    const over2 = limiter.tryAcquire(1);
    assert.equal(over2.allowed, false);
    assert.equal(over2.resetMs, 600, 'resetMs at t=1400 should be 600ms');

    // Advance past t=2000: capacity reopens
    currentTime = 2000;
    const allowed = limiter.tryAcquire(1);
    assert.equal(allowed.allowed, true);
    assert.equal(allowed.resetMs, 0);
  });

  it('destroy() terminates timer, aborts pending waiters, and rejects subsequent acquisitions', async () => {
    let currentTime = 1000.0;
    const limiter = new SlidingWindowRateLimiter({
      limit: 1,
      windowMs: 1000,
      nowFn: () => currentTime
    });

    limiter.tryAcquire(1);
    const queuedPromise = limiter.acquire(1, 5000);

    limiter.destroy();

    await assert.rejects(queuedPromise, {
      name: 'Error',
      message: 'Rate limiter destroyed'
    });

    // Subsequent calls must immediately reject/throw
    assert.throws(() => limiter.tryAcquire(1), {
      name: 'Error',
      message: 'Rate limiter destroyed'
    });

    await assert.rejects(limiter.acquire(1), {
      name: 'Error',
      message: 'Rate limiter destroyed'
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

  it('throttles probe concurrency in HALF_OPEN when saturated', async () => {
    let currentTime = 1000.0;
    const breaker = new CircuitBreaker({
      failureThresholdPct: 50,
      minimumRequests: 2,
      recoveryTimeoutMs: 500,
      halfOpenSuccessThreshold: 1, // Only 1 concurrent probe allowed
      nowFn: () => currentTime
    });

    await assert.rejects(breaker.execute(async () => { throw new Error('Fail'); }));
    await assert.rejects(breaker.execute(async () => { throw new Error('Fail'); }));

    currentTime += 600;
    assert.equal(breaker.getState(), CircuitState.HALF_OPEN);

    // Start a probe that stays pending
    let resolveProbe;
    const pendingProbePromise = breaker.execute(() => new Promise((res) => { resolveProbe = res; }));

    // Second concurrent request should be rejected as probe saturated
    await assert.rejects(
      breaker.execute(async () => 'BURST'),
      { name: 'Error', message: 'Circuit breaker is HALF_OPEN and probe capacity is saturated' }
    );

    // Fallback should work when saturated
    const fallbackResult = await breaker.execute(
      async () => 'BURST',
      async (err) => `HANDLED_${err.message}`
    );
    assert.equal(fallbackResult, 'HANDLED_Circuit breaker is HALF_OPEN and probe capacity is saturated');

    // Resolve the active probe
    resolveProbe('SUCCESS');
    await pendingProbePromise;
    assert.equal(breaker.getState(), CircuitState.CLOSED);
  });

  it('allows manual reset to CLOSED state and purges historical failure metrics', async () => {
    let currentTime = 1000.0;
    const breaker = new CircuitBreaker({
      failureThresholdPct: 50,
      minimumRequests: 2,
      nowFn: () => currentTime
    });

    await assert.rejects(breaker.execute(async () => { throw new Error('Fail'); }));
    await assert.rejects(breaker.execute(async () => { throw new Error('Fail'); }));
    assert.equal(breaker.getState(), CircuitState.OPEN);

    breaker.reset();
    assert.equal(breaker.getState(), CircuitState.CLOSED);
    assert.deepEqual(breaker.getMetrics(), {
      state: CircuitState.CLOSED,
      totalRecorded: 0,
      successes: 0,
      failures: 0,
      failurePct: 0
    });

    // Single failure after reset must not re-trip because minimumRequests = 2
    await assert.rejects(breaker.execute(async () => { throw new Error('Subsequent fault'); }));
    assert.equal(breaker.getState(), CircuitState.CLOSED, 'Should remain CLOSED because only 1 request recorded');

    const result = await breaker.execute(async () => 'RECOVERED');
    assert.equal(result, 'RECOVERED');
  });
});
