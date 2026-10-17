# ADR 0001 — Sub-bucket sliding window instead of a full request log

**Status:** accepted

## Context

A pure sliding-window log stores every request timestamp (O(n) memory per window). A fixed window counter is cheap but permits burst spiking at window boundaries. The ticket mandates `granularBuckets = 10`, which points at the hybrid sliding-window-counter approach.

## Decision

Store one counter per sub-bucket (bucket index = floor(elapsedMs / (windowMs / granularBuckets))). The window's load is the sum of live buckets; buckets falling out of the window are evicted. This bounds memory at `granularBuckets` entries and approximates the exact log with bounded error, matching the canonical sliding-window-counter design ([Arpit Bhayani, sliding window rate limiter](https://arpitbhayani.me/blogs/sliding-window-ratelimiter/)).

Timing uses `process.hrtime.bigint()` converted to fractional milliseconds for microsecond precision; `resetMs` is derived from the oldest live bucket's expiry, not a fixed window reset.

## Consequences

- `remaining` and `resetMs` are exact with respect to the bucket model, not the true per-request log.
- Deterministic tests must drive time through an injectable clock so sub-bucket eviction is testable without wall-clock sleeps.
