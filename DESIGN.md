# Design Notes

Companion to [`README.md`](README.md). Written as the design brief I'd hand
a reviewing engineer: what we built, what we deliberately did not, and the
decisions I'd revisit before promoting this to production.

---

## 1. Architecture at a glance

```
                 ┌──────────────────┐
   JSON file ─▶  │TransactionSource │ ── read() ──▶ Iterable<TransactionRequest>
                 └──────────────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │BackgroundIngestor│ for each request: processor.submit(r)
                 └──────────────────┘                    then processor.drainPending()
                          │
                          ▼
       ┌─────────────────────────────────────────────────────────┐
       │                TransactionProcessor                     │
       │                                                         │
       │   1. validate (TransactionValidator)                    │
       │   2. dedupe by transactionId  (TransactionStateStore)   │
       │   3. dedupe by requestId      (IdempotencyStore.SETNX)  │
       │   4. order gate (AccountSequenceTracker, per account)   │
       │   5. apply with retry (LedgerStore + RetryPolicy +      │
       │                        FailureSimulator)                │
       │                                                         │
       └─────────────────────────────────────────────────────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │  LedgerStore     │  inner idempotency guard: a second apply
                 │  (in-memory)     │  of the same transactionId is a no-op.
                 └──────────────────┘
```

The processor depends on **interfaces** (`TransactionSource`,
`IdempotencyStore`, `TransactionStateStore`, `LedgerStore`), not concrete
implementations. Swap any of the in-memory impls for a Kafka consumer, a
Postgres-backed ledger, a Redis idempotency store — the core doesn't change.

## 2. Idempotency design — four stacked guards

A single idempotency layer is one database outage away from a double post.
We stack four, outer to inner:

1. **Boundary validation** (`TransactionValidator`). Rejects nulls, negative
   amounts, unsupported currencies before any state is touched. Guarantees
   malformed records never reach the ledger.
2. **transactionId-level dedupe**. Before anything else, we check
   `TransactionStateStore.find(transactionId)`. If a record exists in **any**
   status (even `PENDING_ORDER` or `FAILED`), the new arrival is marked
   `DUPLICATE`. This catches a client that regenerated its idempotency key
   but re-sent the same business transaction.
3. **requestId-level atomic dedupe** (`IdempotencyStore.markSeen`). The
   contract is SETNX-style: `markSeen` returns `true` to exactly one caller;
   all other callers with the same `requestId` get `false` and short-circuit
   as `DUPLICATE`. In the in-memory impl this is a `ConcurrentHashMap.putIfAbsent`;
   in production this is a `UNIQUE` constraint on a dedicated `idempotency_keys`
   row, or `SET key NX` in Redis.
4. **Inner ledger guard** (`LedgerStore.apply`). The last line of defence.
   Before mutating the balance, `apply` adds the transactionId to a set of
   applied transactions; if it was already present, it returns `false` and
   does nothing. This matters because a crash **between** the balance write
   and the processor marking the record `PROCESSED` would otherwise re-apply
   on recovery. The `RetryTest.innerLedgerIdempotency_preventsDoubleApplyAcrossRetries`
   test exercises exactly this: ledger succeeds, "ack" is lost, retry re-enters
   `apply`, guard returns false, balance is unchanged.

The same final business outcome holds for every number of replays — this is
the FDE-level invariant we're defending.

## 3. Duplicate detection logic

Two orthogonal keys, intentionally:

- `requestId` (idempotency key) — the thing the client controls. Catches
  client-side retries after a timeout.
- `transactionId` (business key) — the thing the domain controls. Catches
  "same business transaction, new idempotency key" (which happens with
  buggy clients and with upstream systems that generate a new key on each
  internal retry).

If either match, the second arrival is `DUPLICATE`. The sample input's
`REQ-2-DUP` row exercises both: its `requestId` is new, but its
`transactionId` (`TXN-2002`) is already `PROCESSED`, so the transactionId
check catches it.

Counts are tracked in `ProcessingReport` (per request event), not in the
state store (per transactionId) — this is why a duplicate replay shows up
in the summary even though the state store only holds one record per
business transaction.

## 4. Out-of-order handling strategy — Option A (sequence-based)

**Choice.** Per-account strict-forward sequence (`AccountSequenceTracker`).
Each account starts at `startingSequence` (default `1`). Arrivals at
`expected` release immediately; higher sequences are parked in a per-account
`TreeMap<Long, TransactionRecord>`; lower sequences are returned to the
processor and typically caught by dedupe.

**Why Option A over the alternatives.**
- **Option B (state-based)** is strongest for domains with explicit valid
  transitions (e.g. `CREATED → AUTHORIZED → CAPTURED`). For a flat ledger
  with CREDIT/DEBIT/REVERSAL, the state is just the running balance; there
  isn't much lifecycle to validate against. Overkill for this shape.
- **Option C (latest-state wins)** is correct for state-replace domains
  (user profile updates, device shadow). For money, "stale updates are
  ignored" is unsafe: a late DEBIT is still money moved.
- Option A gives us a crisp, testable invariant: for every account,
  processed sequences form a contiguous `[startingSequence, maxSeq]` range.

**Trade-off I accepted.** An account whose first observed event is at
`seq > startingSequence` (e.g. `ACC-203` seq 5 in the sample) stays parked
until end-of-batch, when `drainPending()` releases it. That's a pragmatic
batch-oriented choice: a true streaming service should instead apply a
**time-based grace window** (e.g. "release the oldest pending for an account
if no new arrivals for 10 s") and route still-missing predecessors to a
**dead-letter queue** with ops-level visibility. The hook for that is a
clean one-method extension of `AccountSequenceTracker`.

**What I did NOT do.** No full event-sourcing, no vector clocks, no
cross-account global ordering. The brief explicitly says these are out of
scope and that a simple, well-explained approach is acceptable.

## 5. Retry and failure handling

- Two kinds of failure, cleanly split:
  - `TransientProcessingException` → retry with bounded backoff.
  - Any other `RuntimeException` → terminal `FAILED`, no retry.
- `RetryPolicy(maxAttempts=3, baseBackoffMillis=10)` — linear backoff in
  this build. Production: exponential + jitter, and surface backoff as a
  metric so SRE can see downstream pressure.
- Retries happen **in-process** inside `processWithRetry`. In production
  the correct shape is a persistent retry queue with the same safety
  guarantees (SQS redrive, Kafka DLT, outbox + scheduled worker). The
  in-process variant is deliberately simple here; the inner ledger guard
  means switching to an out-of-process retry queue doesn't break
  correctness.
- After `maxAttempts` the record moves to `FAILED` with the last failure
  reason recorded on the `TransactionRecord`. It is **not** silently
  dropped — the operator sees it in both the summary and the per-record
  dump of `ReportPrinter`.
- `FailureSimulator` exists only to exercise the retry path
  deterministically. It's wired by test config (`Set<String>` of
  "fail-once" transactionIds) and is disabled by default in production
  configs.

## 6. Observability approach

- **Structured logs** at every meaningful transition: `Received`,
  `Parked`, `Processed`, `Transient failure, retrying`,
  `Max retries exhausted`, `Duplicate ...`, `Validation failed`.
  Fields are logged in `k=v` form (`tx=... req=... account=... seq=... status=... attempt=...`)
  so they're grep-friendly today and trivial to upgrade to JSON lines
  (switch logback encoder) when fed to Splunk/Loki/Datadog.
- **Correlation**: every log line carries both `tx` (business
  transactionId) and `req` (idempotency requestId). The reviewer can
  trace any transaction end-to-end from a single grep.
- **End-of-batch summary** via `ReportPrinter` prints counts by status,
  retry totals, per-record final state, and the ledger snapshot. These are
  the three views an on-call engineer actually asks for.
- **What's missing**: a real metrics pipeline (Prometheus counters/
  histograms for processed/duplicate/failed/retry; p99 latency per status).
  See §9 production hardening.

## 7. Security and data handling

- **No PII in logs**: the structured log templates deliberately never
  reference `amount` or `currency` — only identifiers (`transactionId`,
  `requestId`, `accountId`) and operational metadata (status, attempt).
  That trade-off keeps logs useful without leaking financial values into
  centralised log stores.
- **Input validation at the boundary**: `TransactionValidator` is the
  only place where a malformed record is permitted to exist as data; past
  that point every component assumes well-formed input.
- **No secrets in source**: there are none to begin with, and
  `.gitignore` excludes `target/` and IDE state.
- **Safe error handling**: `BackgroundIngestor` wraps each `submit` in
  try/catch so a single poison record cannot halt the batch —
  `ValidationTest.malformedRecord_isIsolated_andNextRecordStillProcesses`
  verifies this.

## 8. Concurrency model

- The CLI ingestor is single-threaded by design: it processes records
  in arrival order, which keeps the sequence tracker's semantics obvious.
- Every store and the processor's safety nets are nonetheless
  **thread-safe** (`ConcurrentHashMap`, atomic `putIfAbsent`, `synchronized`
  blocks on the ledger). Moving to a worker-pool model where each
  **account** has its own worker is a small, mechanical change — the
  tracker, idempotency store, and ledger guard were built with this in mind.

## 9. Production hardening — what I'd add before shipping

Ordered by what buys the most risk reduction per unit of effort.

1. **Durable idempotency + state store.** Replace `InMemory*Store` with a
   Postgres schema: `idempotency_keys(request_id PK)`,
   `transactions(transaction_id PK, status, attempt_count, ...)`,
   `ledger_entries(transaction_id PK, account_id, amount, direction)`.
   The processor's "apply ledger + mark PROCESSED" step becomes a single
   DB transaction; the `UNIQUE` constraint on `ledger_entries.transaction_id`
   is the inner guard.
2. **Real queue adapter.** `TransactionSource` behind a Kafka or SQS
   consumer, with explicit ack-after-processing semantics. Combined with
   #1, a crash between apply and ack is safe because replay hits the
   `UNIQUE` constraint and the record is already in `PROCESSED`.
3. **Dead-letter queue.** For records stuck in `PENDING_ORDER` past a
   grace window, and for records that exhaust retries. Must carry enough
   context for a human to triage without pulling the full ledger.
4. **Time-based grace window** in `AccountSequenceTracker` (as discussed
   in §4) so streaming use doesn't rely on batch-end drain.
5. **Metrics**. Prometheus counters per status, retry totals, pending-
   order gauge per account; Grafana dashboard and alerts on failure rate
   and DLQ depth.
6. **Backpressure and bulkheads**. Per-account worker + bounded queue
   per worker; failures on one account don't starve others.
7. **Schema versioning**. Add `schemaVersion` to `TransactionRequest`;
   validator is strict against the current major, lenient against minor.
8. **Secret handling**. Config loaded from a secrets manager (SOPS,
   AWS SM) rather than static defaults.

## 10. AI-assisted development usage

- Used an AI pair-programmer (Claude Code / Claude 4.7) to produce the
  bulk of the Java scaffolding and the first-pass test cases.
- I kept the AI on short scopes: one module at a time, each prompted with
  the exact invariants it had to preserve (e.g. "the ledger must be
  idempotent on transactionId even when the processor retries after a
  simulated post-apply crash"). The invariants came from the brief; the
  AI translated them to code.
- **How the generated code was validated**:
  - Compile-check after each component (`mvn compile`) and re-check after
    every change.
  - Deliberately failed one accessor name collision early (record
    component named `valid` with static factory also named `valid()`) —
    kept the fix narrow (rename factory to `ok()`) rather than let the AI
    "fix" the model.
  - **Tests written before I trusted the output**. The L2 sample-table
    scenario (`BatchIngestionTest`) and the "ack lost after apply"
    scenario (`RetryTest.innerLedgerIdempotency_preventsDoubleApplyAcrossRetries`)
    were the two I used to challenge the AI's first-cut design; both
    forced tightening of the inner ledger guard.
  - **No blind acceptance of generated code**. I rejected:
    - a Spring Boot scaffolding suggestion (unneeded surface area for the
      stated scope);
    - an over-eager Option-B "state machine with transition table" that
      didn't match a flat ledger domain;
    - a first-cut `AccountSequenceTracker` that eagerly released the
      first-seen seq (would have mis-processed TXN-2001 as "late").
- **Where AI genuinely helped**: boilerplate (records, builders), JUnit
  5 test harness repetition, SLF4J log templates, the `ReportPrinter`
  formatting. **Where human judgement drove the design**: the four-layer
  idempotency stack, Option A vs B vs C, the batch-end drain trade-off,
  and the "no PII in logs" rule.
