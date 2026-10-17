# 02: Adaptive Concurrency Limiter with TCP Vegas RTT Gradient & AbortSignal

**Status:** done
**Blocked by:** None
**Priority:** High
**Owner:** Spark / 5090 Blackwell (:8877) + Double-Blind Audit (:8080)

## Context & Objectives
In high-throughput microservices, static rate limits either under-utilize capacity or cause cascading queue collapses during downstream latency spikes.
Implement an **Adaptive Concurrency Limiter (`AdaptiveConcurrencyLimiter`)** in `src/adaptive-limiter.js` based on TCP Vegas / Little's Law RTT gradient estimation, with full `AbortSignal` cancellation support and zero counter leaks.

## Architectural Boundaries
- Source code in `src/adaptive-limiter.js` and exported via `src/index.js`.
- Test suite in `test/adaptive.test.js` using Node's native test runner (`node:test` and `node:assert/strict`).
- Zero external dependencies.
- Monotonic clock seam via `nowFn = hrtimeMs` (from `src/clock.js`).

## Component Specifications: `AdaptiveConcurrencyLimiter`
- Constructor options:
  - `minLimit`: Minimum allowable concurrency limit (default: 1, positive integer).
  - `maxLimit`: Maximum allowable concurrency limit (default: 20, positive integer).
  - `initialLimit`: Initial concurrency limit (default: 5).
  - `smoothingFactor`: EWMA smoothing factor for RTT tracking (default: 0.2, between 0.05 and 0.95).
  - `backoffFactor`: Multiplicative decrease factor on latency spikes (default: 0.7, between 0.1 and 0.9).
  - `tolerance`: RTT gradient tolerance threshold before throttling (default: 1.25).
  - `nowFn`: Injectable monotonic clock function returning fractional milliseconds.

### Core Methods:
1. `execute(actionFn, { signal = null } = {})`:
   - Enforces active in-flight concurrency <= current limit.
   - If in-flight requests >= current limit, immediately rejects with `Error('Concurrency limit reached')`.
   - Supports optional standard `AbortSignal`:
     - If already aborted prior to call, immediately rejects with `signal.reason || new Error('The operation was aborted')` without incrementing in-flight count.
     - If aborted while `actionFn` is executing, rejects with `signal.reason || new Error('The operation was aborted')`.
     - **CRITICAL TRAP / INVARIANT**: The in-flight concurrency counter (`inFlight`) MUST NEVER leak under any circumstance (normal completion, runtime throw, or mid-flight abort). `inFlight` must accurately return to 0 when all actions finish.
   - RTT Gradient & Limit Adaptation:
     - Measures execution duration using `nowFn`.
     - Tracks `baseRtt` (minimum observed RTT over healthy requests).
     - Updates smoothed `currentRtt` using EWMA: `smoothedRtt = (1 - alpha) * smoothedRtt + alpha * sampleRtt`.
     - Computes RTT gradient: `gradient = smoothedRtt / baseRtt`.
     - If `gradient <= tolerance`: Additive Increase (`limit = Math.min(maxLimit, limit + 1)`).
     - If `gradient > tolerance` (downstream queuing detected): Multiplicative Decrease (`limit = Math.max(minLimit, Math.floor(limit * backoffFactor))`).
2. `getStats()`:
   - Returns `{ limit: number, inFlight: number, baseRtt: number, smoothedRtt: number }`.
3. `reset()`:
   - Resets `limit` to `initialLimit`, `inFlight` to 0, and clears RTT history.

## Test Specifications (`test/adaptive.test.js`)
1. Allows concurrent requests up to current limit and rejects excess with `'Concurrency limit reached'`.
2. Increases concurrency limit additively on low-latency executions.
3. Decreases concurrency limit multiplicatively on latency spikes, respecting `minLimit`.
4. Handles `AbortSignal` without leaking in-flight concurrency:
   - Pre-aborted signal rejects immediately without mutating `inFlight`.
   - Mid-flight abort rejects with abort reason and decrements `inFlight` cleanly back to 0.
5. All 5 test cases passing under `node --test` with exit code 0.
