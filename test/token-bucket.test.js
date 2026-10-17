import test from 'node:test';
import assert from 'node:assert/strict';
import { TokenBucketLimiter } from '../src/index.js';

test('TokenBucketLimiter - constructor validation', () => {
  const limiter = new TokenBucketLimiter();
  assert.strictEqual(limiter.capacity, 10);
  assert.strictEqual(limiter.refillRate, 5);
  assert.strictEqual(limiter.peek(), 10);

  // Boundary checks
  assert.throws(() => new TokenBucketLimiter({ capacity: 0 }), /Capacity must be a positive finite number/);
  assert.throws(() => new TokenBucketLimiter({ capacity: -5 }), /Capacity must be a positive finite number/);
  assert.throws(() => new TokenBucketLimiter({ capacity: NaN }), /Capacity must be a positive finite number/);
  assert.throws(() => new TokenBucketLimiter({ refillRate: 0 }), /Refill rate must be a positive finite number/);
  assert.throws(() => new TokenBucketLimiter({ refillRate: -1 }), /Refill rate must be a positive finite number/);
  assert.throws(() => new TokenBucketLimiter({ initialTokens: NaN }), /Initial tokens must be a non-negative finite number/);
  assert.throws(() => new TokenBucketLimiter({ initialTokens: -1 }), /Initial tokens must be a non-negative finite number/);
});

test('TokenBucketLimiter - argument validation on tryConsume', () => {
  const limiter = new TokenBucketLimiter();
  assert.throws(() => limiter.tryConsume(0), /Tokens must be a positive finite number/);
  assert.throws(() => limiter.tryConsume(-1), /Tokens must be a positive finite number/);
  assert.throws(() => limiter.tryConsume(NaN), /Tokens must be a positive finite number/);
  assert.throws(() => limiter.tryConsume(Infinity), /Tokens must be a positive finite number/);
  assert.throws(() => limiter.tryConsume('invalid'), /Tokens must be a positive finite number/);
});

test('TokenBucketLimiter - burst consumption and exhaustion', () => {
  let simulatedTime = 1000.0;
  const limiter = new TokenBucketLimiter({
    capacity: 10,
    refillRate: 2,
    nowFn: () => simulatedTime
  });

  // Consume full burst
  const res1 = limiter.tryConsume(10);
  assert.strictEqual(res1.allowed, true);
  assert.strictEqual(res1.remaining, 0);
  assert.strictEqual(res1.retryAfterMs, 0);

  // Second consume immediately fails
  const res2 = limiter.tryConsume(1);
  assert.strictEqual(res2.allowed, false);
  assert.strictEqual(res2.remaining, 0);
  // Need 1 token at 2 tokens/sec = 500ms
  assert.strictEqual(res2.retryAfterMs, 500);

  // Advance clock by 250ms -> only 0.5 tokens accrued, still not enough for 1 token
  simulatedTime += 250;
  const res3 = limiter.tryConsume(1);
  assert.strictEqual(res3.allowed, false);
  assert.strictEqual(res3.retryAfterMs, 250);

  // Advance clock by another 250ms (total 500ms) -> 1 token accrued
  simulatedTime += 250;
  const res4 = limiter.tryConsume(1);
  assert.strictEqual(res4.allowed, true);
  assert.strictEqual(res4.retryAfterMs, 0);
});

test('TokenBucketLimiter - ceiling clamp at capacity', () => {
  let simulatedTime = 1000.0;
  const limiter = new TokenBucketLimiter({
    capacity: 5,
    refillRate: 10,
    nowFn: () => simulatedTime
  });

  // Advance time by 100 seconds (would be 1000 tokens)
  simulatedTime += 100000;
  assert.strictEqual(limiter.peek(), 5);

  const res = limiter.tryConsume(5);
  assert.strictEqual(res.allowed, true);
  assert.strictEqual(res.remaining, 0);
});

test('TokenBucketLimiter - reset restores initial state', () => {
  let simulatedTime = 1000.0;
  const limiter = new TokenBucketLimiter({
    capacity: 10,
    initialTokens: 3,
    refillRate: 1,
    nowFn: () => simulatedTime
  });

  assert.strictEqual(limiter.peek(), 3);
  limiter.tryConsume(3);
  assert.strictEqual(limiter.peek(), 0);

  limiter.reset();
  assert.strictEqual(limiter.peek(), 3);
});
