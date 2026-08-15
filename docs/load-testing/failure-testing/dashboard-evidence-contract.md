# Dashboard Evidence Contract

This contract defines how dashboards interpret continuous loadgen observation.
Fault injection and fault timestamps are external inputs. Loadgen does not emit
a campaign verdict.

## Independent dimensions

Every selected query window reports three independent dimensions:

- Evidence: `VALID` or `INCONCLUSIVE`.
- Impact: `NO_IMPACT`, `TRANSIENT_RECOVERED`, or `UNRECOVERED`.
- Correctness: `CLEAN` or `CONFIRMED_VIOLATION`.

Positive evidence survives incomplete observation. For example, a retained
content mismatch remains `CONFIRMED_VIOLATION` during an unrelated scrape gap.
Absence claims (`CLEAN`, `NO_IMPACT`, or no missing recipients) require valid,
complete evidence. A missing series is unknown, never zero.

## Query cadence and freshness

- Prometheus scrape interval: 30 seconds.
- Evaluation lookback: 2 minutes.
- Evaluation step: 1 minute.
- Minimum samples per required series in each lookback: 3.
- Recovery: 5 consecutive healthy evaluation points.
- Minimum post-remediation evaluation window: 6 minutes.

A missing, stale, truncated, or undersampled required series makes the
dependent absence claim `INCONCLUSIVE`. Queries must not use `or vector(0)` for
required evidence.

## Dispatch validity

The dispatch ratio for each enabled lane is:

```text
actual   = increase(loadgen_soak_dispatched_total{lane="$lane"}[2m])
expected = loadgen_soak_configured_rate{lane="$lane"} * 120
valid    = actual * 100 >= expected * 95
```

The exact 95% boundary is valid. `loadgen_soak_intended_total` is an observed
pacing counter, not a replacement for the stable target calculation. Its
identity must hold over the same range:

```text
increase(intended[2m])
  == increase(dispatched[2m])
   + increase(scheduler_underrun[2m])
   + increase(lane_saturation[2m])
   + increase(global_saturation[2m])
```

Attribution rules:

- process down, scrape gaps, or scheduler underrun: evidence is inconclusive;
- lane/global saturation: absence claims are inconclusive and positive impact
  remains reportable;
- unattributed dispatch shortfall: evidence is inconclusive;
- `not_sent` is already a dispatched publish lifecycle outcome and is not
  subtracted from dispatch.

## Observer validity

Configured observers are declared by
`loadgen_failure_observer_configured{observer}`. Eligibility is counted by
`loadgen_failure_observer_eligible_total{scenario,lane,observer}`. A disabled
observer is not eligible and cannot make the interval unverified.

For each configured observer and lane:

```text
eligible   = increase(loadgen_failure_observer_eligible_total[2m])
unverified = increase(loadgen_failure_observations_total{result="unverified"}[2m])
limit      = max(3, ceil(0.001 * eligible))
invalid    = unverified > limit
```

The numerator and denominator must both be displayed. An observer blind
interval overlapping an operation observation window invalidates an absence
claim for that operation. Startup-down time, disconnects, queue overflow,
truncated health history, and stale health all count as blind evidence. A
healthy authoritative not-found at or after deadline may produce
`missing_after_deadline`; timeout or observer unavailability produces
`unverified`.

## Result interpretation

Normalized operation results are `good`, `bad`, `unverified`, `not_sent`, and
`missing_after_deadline`. Observation results omit `not_sent`.

Bounded result reasons include:

- `admission_rejected`;
- `history_content_mismatch` and `history_missing`;
- `recipient_duplicate`, `recipient_unexpected`,
  `recipient_identity_mismatch`, and `recipient_missing`;
- `publish_local_error` for proven local pre-publish `not_sent` only.

`bad`, authoritative missing, unexpected recipients, duplicates, and identity
mismatches are positive correctness evidence. `unverified`, WAL invalidation,
untracked operations, dropped recovery records, observer blind intervals, and
scrape gaps invalidate absence claims.

## Metric inputs and ownership

Existing loadgen metrics reused by this contract:

- `loadgen_failure_operations_total`,
  `loadgen_failure_observations_total`, `loadgen_failure_inflight`,
  `loadgen_failure_recovered_operations_total`,
  `loadgen_failure_invalidations_total`, `loadgen_failure_journal_bytes`,
  `loadgen_failure_untracked_total`, and `loadgen_failure_dropped_total`;
- `loadgen_failure_observer_up`, `loadgen_failure_observer_events_total`,
  `loadgen_failure_observer_queue_depth`, `loadgen_nats_connected`,
  `loadgen_nats_connection_events_total`,
  `loadgen_nats_outage_duration_seconds`, and
  `loadgen_nats_current_outage_seconds`;
- `loadgen_consumer_sample_errors_total`, `loadgen_run_info`, process metrics,
  and Go runtime metrics.

Metrics added by this work:

- `loadgen_soak_configured_rate`, `loadgen_soak_intended_total`,
  `loadgen_soak_dispatched_total`, `loadgen_soak_scheduler_underrun_total`,
  `loadgen_soak_lane_saturation_total`, and
  `loadgen_soak_global_saturation_total`;
- `loadgen_failure_observer_configured` and
  `loadgen_failure_observer_eligible_total`;
- `loadgen_failure_observation_reasons_total` and
  `loadgen_failure_not_sent_total`.

Externally owned metrics include application service metrics, NATS/JetStream
exporter state, NATS topology, leader, and quorum state, Kubernetes restart/OOM
status, scrape health, and operator fault-window annotations. Loadgen does not
synthesize or own these series, and their absence is not synthesized as
success.

Allowed hot-path label values are code-owned registries: scenario is fixed to
`cassandra_soak`; lanes, observers, results, reasons, NATS pools/events, and
consumer identifiers are bounded configuration values. Run, operation,
message, room, account, user, recipient, subject, inbox, raw error/advisory,
and pod UID values are forbidden labels.

## Recovery classification

Starting at the external remediation timestamp, evaluate each full 2-minute
lookback at 1-minute steps. `TRANSIENT_RECOVERED` requires five consecutive
healthy points and at least six minutes of post-remediation data. Any failing
point resets the streak. If impact remains at the end of the selected window,
classify it as `UNRECOVERED`; insufficient evidence remains independently
`INCONCLUSIVE`.

The dashboard should expose lookback, evaluation step, and required healthy
points as visible variables and automatically extend the selected window far
enough to evaluate recovery. If the selected window ends before the minimum
post-remediation duration, recovery is unconfirmed rather than recovered.
