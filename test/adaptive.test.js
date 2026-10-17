import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { AdaptiveConcurrencyLimiter } from '../src/index.js';

describe('AdaptiveConcurrencyLimiter', () => {
  it('enforces concurrency limit and rejects excess', async () => {
    let currentTime = 1000.0;
    const limiter = new AdaptiveConcurrencyLimiter({
      initialLimit: 2,
      minLimit: 1,
      maxLimit: 5,
      nowFn: () => currentTime
    });

    let resolve1, resolve2;
    const p1 = limiter.execute(() => new Promise((res) => { resolve1 = res; }));
    const p2 = limiter.execute(() => new Promise((res) => { resolve2 = res; }));

    assert.equal(limiter.getStats().inFlight, 2);

    // 3rd concurrent request must reject with Concurrency limit reached
    await assert.rejects(
      limiter.execute(async () => 'EXCESS'),
      { name: 'Error', message: 'Concurrency limit reached' }
    );

    resolve1('DONE_1');
    resolve2('DONE_2');
    await Promise.all([p1, p2]);
    assert.equal(limiter.getStats().inFlight, 0);
  });

  it('adapts limit additively on low latency and multiplicatively on latency spikes', async () => {
    let currentTime = 1000.0;
    const limiter = new AdaptiveConcurrencyLimiter({
      initialLimit: 5,
      minLimit: 2,
      maxLimit: 10,
      backoffFactor: 0.5,
      tolerance: 1.2,
      nowFn: () => currentTime
    });

    // Establish base RTT of 50ms
    currentTime = 1000.0;
    await limiter.execute(async () => { currentTime += 50; return 'OK'; });
    assert.equal(limiter.getStats().baseRtt, 50);

    // Another fast request: RTT 50ms (gradient = 1.0 <= 1.2) -> Additive increase from 5 to 6
    currentTime = 2000.0;
    await limiter.execute(async () => { currentTime += 50; return 'OK'; });
    assert.equal(limiter.getStats().limit, 6);

    // Latency spike: RTT 300ms (gradient > 1.2) -> Multiplicative decrease: floor(6 * 0.5) = 3
    currentTime = 3000.0;
    await limiter.execute(async () => { currentTime += 300; return 'OK'; });
    assert.equal(limiter.getStats().limit, 3);
  });

  it('handles AbortSignal without leaking in-flight concurrency counter', async () => {
    let currentTime = 1000.0;
    const limiter = new AdaptiveConcurrencyLimiter({
      initialLimit: 3,
      nowFn: () => currentTime
    });

    // 1. Pre-aborted signal
    const preController = new AbortController();
    preController.abort(new Error('Pre-aborted'));

    await assert.rejects(
      limiter.execute(async () => 'NEVER', { signal: preController.signal }),
      { name: 'Error', message: 'Pre-aborted' }
    );
    assert.equal(limiter.getStats().inFlight, 0, 'Pre-aborted request must not increment inFlight');

    // 2. Mid-flight abort
    const midController = new AbortController();
    let resolveAction;
    const midPromise = limiter.execute(
      () => new Promise((res) => { resolveAction = res; }),
      { signal: midController.signal }
    );

    assert.equal(limiter.getStats().inFlight, 1, 'In-flight should be 1 while action is running');

    // Abort while running
    midController.abort(new Error('Mid-flight cancellation'));

    await assert.rejects(midPromise, {
      name: 'Error',
      message: 'Mid-flight cancellation'
    });

    // CRITICAL: In-flight must return to 0 immediately upon abort!
    assert.equal(limiter.getStats().inFlight, 0, 'In-flight counter must NEVER leak on mid-flight abort');

    // Resolve after abort to verify no zombie state mutations
    resolveAction('LATE');
    assert.equal(limiter.getStats().inFlight, 0);
  });

  it('validates constructor options and actionFn parameter', async () => {
    assert.throws(() => new AdaptiveConcurrencyLimiter({ minLimit: -1 }), { name: 'Error' });
    assert.throws(() => new AdaptiveConcurrencyLimiter({ minLimit: 10, maxLimit: 5 }), { name: 'Error' });
    assert.throws(() => new AdaptiveConcurrencyLimiter({ initialLimit: 100, maxLimit: 10 }), { name: 'Error' });
    assert.throws(() => new AdaptiveConcurrencyLimiter({ smoothingFactor: 1.5 }), { name: 'Error' });
    assert.throws(() => new AdaptiveConcurrencyLimiter({ backoffFactor: 0 }), { name: 'Error' });

    const limiter = new AdaptiveConcurrencyLimiter();
    await assert.rejects(limiter.execute('NOT_A_FUNCTION'), { name: 'TypeError' });
  });

  it('resets limit to initialLimit and clears RTT history on reset()', async () => {
    let currentTime = 1000.0;
    const limiter = new AdaptiveConcurrencyLimiter({
      initialLimit: 5,
      minLimit: 1,
      maxLimit: 10,
      nowFn: () => currentTime
    });

    // Run low latency request to adapt limit to 6
    await limiter.execute(async () => { currentTime += 10; return 'OK'; });
    await limiter.execute(async () => { currentTime += 10; return 'OK'; });
    assert.equal(limiter.getStats().limit, 6);

    limiter.reset();
    const stats = limiter.getStats();
    assert.equal(stats.limit, 5, 'Limit should be restored to initialLimit');
    assert.equal(stats.inFlight, 0);
    assert.equal(stats.baseRtt, 0);
    assert.equal(stats.smoothedRtt, 0);
  });

  it('clamps concurrency strictly between minLimit and maxLimit under extreme surges', async () => {
    let currentTime = 1000.0;
    const limiter = new AdaptiveConcurrencyLimiter({
      initialLimit: 2,
      minLimit: 2,
      maxLimit: 3,
      backoffFactor: 0.5,
      nowFn: () => currentTime
    });

    // Establish base RTT
    await limiter.execute(async () => { currentTime += 10; return 'OK'; });

    // Repeated low latency requests cannot exceed maxLimit = 3
    for (let i = 0; i < 5; i++) {
      await limiter.execute(async () => { currentTime += 10; return 'OK'; });
    }
    assert.equal(limiter.getStats().limit, 3, 'Limit must not exceed maxLimit');

    // Repeated high latency spikes cannot drop below minLimit = 2
    for (let i = 0; i < 5; i++) {
      await limiter.execute(async () => { currentTime += 500; return 'SPIKE'; });
    }
    assert.equal(limiter.getStats().limit, 2, 'Limit must not fall below minLimit');
  });
});
