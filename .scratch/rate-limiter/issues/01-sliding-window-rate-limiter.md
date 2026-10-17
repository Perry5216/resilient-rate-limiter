# 01: Enterprise Sliding Window Rate Limiter and Circuit Breaker
**Status:** done
**Blocked by:** None
**Priority:** High
**Owner:** 5090 Blackwell (:8877) + Double-Blind Audit (:8080)

## Context & Objectives
Implement a zero-dependency, production-grade Sliding Window Token Rate Limiter and State-Machine Circuit Breaker in modern ES Module Node.js with microsecond precision.

## Architectural Boundaries
- Source code in `src/rate-limiter.js`, `src/circuit-breaker.js`, and `src/index.js`.
- Test suite in `test/resilience.test.js` using Node's native test runner (`node:test` and `node:assert/strict`).
- Zero external third-party dependencies (pure standard Node.js).
- Clean separation of concerns: Rate limiter handles capacity throttling; Circuit breaker handles transient fault isolation.

## Component Specifications

### 1. `SlidingWindowRateLimiter` (`src/rate-limiter.js`)
- Constructor options: `{ limit, windowMs, granularBuckets = 10 }`.
- `tryAcquire(tokens = 1)`: Returns `{ allowed: boolean, remaining: number, resetMs: number }`.
- `async acquire(tokens = 1, timeoutMs = 5000)`: Queues request if capacity exhausted; resolves `true` when capacity opens, rejects with `Error('Rate limit acquire timed out')` if `timeoutMs` expires.
- `getStats()`: Returns `{ currentLoad: number, remaining: number, limit: number }`.
- Uses high-resolution timer (`process.hrtime.bigint()`) to calculate elapsed milliseconds precisely.

### 2. `CircuitBreaker` (`src/circuit-breaker.js`)
- Extends `EventEmitter`.
- States: `'CLOSED'`, `'OPEN'`, `'HALF_OPEN'`.
- Constructor options: `{ failureThresholdPct = 50, minimumRequests = 5, recoveryTimeoutMs = 500, halfOpenSuccessThreshold = 2 }`.
- `async execute(actionFn, fallbackFn = null)`:
  - If `OPEN`: Throws `Error('Circuit breaker is OPEN')` or executes `fallbackFn()`.
  - If `HALF_OPEN`: Allows limited probe calls. If `halfOpenSuccessThreshold` reached consecutively, resets to `CLOSED`. If any fail, trips immediately back to `OPEN`.
  - If `CLOSED`: Executes `actionFn()`. Computes failure percentage over rolling window. If failure threshold exceeded after `minimumRequests`, trips to `OPEN`.
- Emits events: `'stateChange'` `({ from, to, timestamp })`, `'trip'`, `'success'`, `'failure'`.

### 3. Exports (`src/index.js`)
- Exports both `SlidingWindowRateLimiter` and `CircuitBreaker`.

### 4. Deterministic Test Suite (`test/resilience.test.js`)
- Must contain comprehensive unit tests covering:
  - Rate limit bursts within window.
  - Rate limit throttling with accurate `remaining` and `resetMs`.
  - Sliding sub-bucket token replenishment.
  - Circuit breaker trip on consecutive failures.
  - Circuit breaker fast-fail when OPEN with fallback invocation.
  - Circuit breaker recovery through HALF_OPEN to CLOSED.
- Tests must pass cleanly with `npm test` / `node --test` (Exit code 0).

