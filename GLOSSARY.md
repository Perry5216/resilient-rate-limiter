# GLOSSARY

## Resilience

**Window** — the rolling span of `windowMs` over which a limiter's `limit` applies. Not a fixed calendar bucket; it slides with each decision.

**Sub-bucket** — one of `granularBuckets` slices inside the Window. A request is registered in the sub-bucket containing its timestamp; the Window's load is the sum of all live sub-buckets. Sub-buckets older than the Window are evicted (they can never re-enter a future Window).

**Load** — the number of tokens currently consumed within the Window (sum of live sub-buckets). `remaining = limit - load`.

**resetMs** — milliseconds until the oldest live sub-bucket leaves the Window, i.e. the earliest moment capacity can reopen.

**Trip** — the transition CLOSED → OPEN. Requires at least `minimumRequests` observations in the rolling failure window AND failure percentage strictly greater than `failureThresholdPct`.

**Probe** — an `execute()` call admitted while HALF_OPEN. Probes are limited to `halfOpenSuccessThreshold` consecutive successes; any probe failure trips immediately back to OPEN.

**Rolling failure window** — the CircuitBreaker's observation ring of the most recent outcomes (success/failure), sized `minimumRequests`. Failure percentage is computed over this ring only after it is full.

**Fast-fail** — an `execute()` call rejected without invoking `actionFn` because the breaker is OPEN; if a `fallbackFn` is supplied it is invoked instead.
