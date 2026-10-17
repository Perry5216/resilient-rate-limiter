# ADR 0002 — Injectable clock (nowFn) for deterministic tests

**Status:** accepted

## Context

The ticket demands a deterministic test suite covering sub-bucket replenishment, HALF_OPEN recovery, and acquire timeouts. Real-time sleeps make those tests flaky and slow; the house rule requires dual verification (assertions + exit code 0).

## Decision

Both components accept an optional `nowFn` (defaults to `hrtimeMs` via `process.hrtime.bigint()` for the limiter, and a monotonic ms clock for the breaker). Tests inject a virtual clock that advances explicitly. Timers for `acquire()` queue draining use the injected clock's notion of elapsed time; in tests the virtual clock is advanced and the queue is drained synchronously via a microtask checkpoint.

## Consequences

- Production behavior is unchanged (default clock is `process.hrtime.bigint()`).
- Tests remain deterministic and fast; no reliance on scheduler timing beyond Node's event-loop ordering.
