# 03: High-Precision Token Bucket Limiter with Fractional Replenishment

**Status:** done
**Blocked by:** 01-sliding-window-rate-limiter.md, 02-adaptive-concurrency-limiter.md
**Priority:** High
**Owner:** Spark / 5090 Blackwell (:8877) + Double-Blind Audit (:8080)

## Context & Objectives
While sliding window limiters smooth out burst traffic over fixed intervals, traffic shaping often requires allowing short-lived bursts up to a hard capacity while enforcing a smooth steady-state replenishment rate.
Implement a **Token Bucket Limiter (`TokenBucketLimiter`)** in `src/token-bucket.js` with fractional token accrual, sub-millisecond clock tracking, and strict parameter validation.

## Architectural Boundaries
- Source code in `src/token-bucket.js` and exported via `src/index.js`.
- Test suite in `test/token-bucket.test.js` using Node's native test runner (`node:test` and `node:assert/strict`).
- Zero external dependencies.
- Monotonic clock seam via `nowFn = hrtimeMs` (from `src/clock.js`).

## Component Specifications: `TokenBucketLimiter`
- Constructor options:
  - `capacity`: Maximum token capacity (must be positive finite number, default: 10).
  - `refillRate`: Tokens added per second (must be positive finite number, default: 5).
  - `initialTokens`: Initial tokens present (default: `capacity`, clamped to `[0, capacity]`).
  - `nowFn`: Monotonic timestamp provider in milliseconds (default: `hrtimeMs`).
- Methods:
  - `tryConsume(tokens = 1)`:
    - Must validate `tokens`: must be a positive finite number (`tokens > 0 && Number.isFinite(tokens)`). Throws `TypeError` or `RangeError` with message `/Tokens must be a positive finite number/` if invalid.
    - Accrues fractional tokens based on elapsed monotonic time since last refill.
    - If `tokens <= available`: deducts tokens and returns `{ allowed: true, remaining: number, retryAfterMs: 0 }`.
    - If `tokens > available`: does not deduct tokens and returns `{ allowed: false, remaining: number, retryAfterMs: number }`, where `retryAfterMs` is the exact milliseconds required to accrue the missing tokens.
  - `peek()`:
    - Returns current available tokens after virtual accrual without deducting.
  - `reset()`:
    - Resets available tokens back to `initialTokens` and resets last refill timestamp.

## Verification Gate
- Test suite in `test/token-bucket.test.js` must verify:
  1. Default constructor configuration and option bounds checking.
  2. Boundary rejection for `NaN`, `<= 0`, `Infinity`, and non-numeric inputs in `tryConsume`.
  3. Precise steady-state replenishment over simulated clock ticks.
  4. Burst allowance up to `capacity`.
  5. Accurate `retryAfterMs` calculation when capacity is exhausted.
  6. Full export via `src/index.js`.
