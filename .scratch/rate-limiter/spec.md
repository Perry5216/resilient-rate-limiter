# Spec: Enterprise Sliding Window Rate Limiter and Circuit Breaker

**Status:** ready-for-agent (ticket `.scratch/rate-limiter/issues/01-sliding-window-rate-limiter.md`)

## Problem Statement

Callers of an unreliable downstream service need capacity throttling (bounded request rate with accurate remaining/reset reporting) and transient fault isolation (fast-fail when the downstream is failing, with controlled recovery probes). Both must run in plain Node.js ESM with no third-party dependencies, and must be deterministically testable.

## Solution

Two cooperating but cleanly separated modules behind small constructors:

- `SlidingWindowRateLimiter` — sub-bucket sliding window (ADR 0001): immediate `tryAcquire`, queue-and-wait `acquire`, stats.
- `CircuitBreaker` — three-state machine (CLOSED/OPEN/HALF_OPEN) over a rolling failure window, EventEmitter-based, with fallback invocation on fast-fail.

`src/index.js` is the single public seam exporting both.

## User Stories

1. As an API gateway, I want `tryAcquire` to allow a burst up to `limit` within the window, so that legitimate traffic passes without latency.
2. As an API gateway, I want `tryAcquire` to deny over-limit requests with exact `remaining` and `resetMs`, so I can emit accurate `Retry-After` headers.
3. As a background worker, I want `acquire()` to queue when capacity is exhausted and resolve when capacity reopens, so throughput is smoothed rather than rejected.
4. As a background worker, I want `acquire()` to reject with `Error('Rate limit acquire timed out')` after `timeoutMs`, so I don't block forever on a saturated limiter.
5. As an operator, I want `getStats()` to report `currentLoad`, `remaining`, and `limit`, so I can monitor saturation.
6. As a tester, I want sub-bucket replenishment to be observable: tokens consumed in an early sub-bucket free up as that bucket slides out of the window, so sliding behavior is provable, not just fixed-window counting.
7. As a resilience engineer, I want the breaker to trip to OPEN only after `minimumRequests` observations AND failure percentage strictly exceeding `failureThresholdPct`, so a single failure never trips it.
8. As a caller, I want fast-fail (`Error('Circuit breaker is OPEN')`) while OPEN, so failing downstreams are not hammered.
9. As a caller, I want an optional `fallbackFn` invoked while OPEN, so degraded responses are possible.
10. As a resilience engineer, I want HALF_OPEN to admit limited probes and require `halfOpenSuccessThreshold` consecutive successes to close, with any failure re-tripping immediately, so recovery is cautious.
11. As an observer, I want `stateChange` (`{ from, to, timestamp }`), `trip`, `success`, `failure` events, so I can instrument and alert.
12. As a tester, I want an injectable clock (ADR 0002) so every timing behavior is deterministic without wall-clock sleeps.

## Implementation Decisions

- **Limiter state model:** `Map<bucketIndex, count>`; bucket width = `windowMs / granularBuckets`; eviction of buckets older than the window on every decision (bounded memory, per ADR 0001).
- **Clock seam:** optional `nowFn` constructor option on both components; default is `process.hrtime.bigint()`-derived fractional ms (microsecond precision). This is the single test seam (highest possible).
- **`resetMs`:** ms until the oldest live bucket expires — the earliest moment capacity can reopen; `0` when capacity is already available.
- **`acquire()` queue:** FIFO of waiters; when a denied `acquire` occurs, compute the earliest bucket-expiry time; drain queue at that time via the injected clock. Timeout uses the same clock; rejection message exactly `Rate limit acquire timed out`.
- **Breaker failure window:** ring buffer of the last `minimumRequests` outcomes; percentage computed only when the ring is full; strict `>` comparison against `failureThresholdPct`.
- **HALF_OPEN probe accounting:** consecutive-success counter; any probe failure → immediate trip; success threshold reached → CLOSED and ring reset.
- **Events:** `stateChange` with `{ from, to, timestamp }` (timestamp from the injected clock), plus `trip`, `success`, `failure`.
- **ESM only**, zero dependencies; `package.json` `"type": "module"`, `npm test` → `node --test`.

## Testing Decisions

- Tests exercise external behavior only: return shapes, state transitions, event emissions, error messages — never internal maps.
- Deterministic virtual clock drives all time-dependent assertions; no `setTimeout`-based sleeps except where the event loop must drain (then bounded and asserted via exit code + assertions).
- Coverage per ticket: burst within window, throttling with accurate `remaining`/`resetMs`, sub-bucket replenishment, trip on failures past `minimumRequests`, fast-fail + fallback while OPEN, HALF_OPEN → CLOSED recovery, HALF_OPEN immediate re-trip, `acquire` queue resolution and timeout rejection.
- Dual verification: assertions plus `node --test` exit code 0.

## Out of Scope

- Distributed/shared state (Redis, consistent hashing), per-key multi-tenant limiters, persistence, metrics backends, Hystrix-style weighted thresholds, and cluster fan-out.

## Further Notes

Grounded against canonical algorithm write-ups: sliding window counter with sub-bucket granularity ([Arpit Bhayani](https://arpitbhayani.me/blogs/sliding-window-ratelimiter/)), algorithm comparison ([api7.ai rate limiting guide](https://api7.ai/blog/rate-limiting-guide-algorithms-best-practices)), and the three-state machine semantics ([resilience4j CircuitBreaker](https://resilience4j.readme.io/docs/circuitbreaker), [groundcover circuit breaker pattern](https://www.groundcover.com/learn/performance/circuit-breaker-pattern)).
