# Walkthrough — How this service is built, end to end

This guide is written for someone who wants to understand or explain the
whole system without being thrown off by jargon. It starts from the
business problem, then goes through the architecture, the component
dependencies, the step-by-step runtime flow, and finishes with the
interview questions you're most likely to get and how to answer them.

Companion to [`README.md`](README.md) (quickstart + FR coverage map) and
[`DESIGN.md`](DESIGN.md) (design decisions + trade-offs).

---

## 1. What this service does, in one paragraph

A bank or payments platform receives a stream of **transaction requests**
(credits, debits, reversals). These requests may arrive **out of order**,
may be **duplicated**, and may fail halfway through processing. This
service accepts those requests and processes them **safely**: it never
double-posts to the ledger, never gets stuck on a bad record, and recovers
cleanly from transient failures. The headline property is **idempotency** —
the same request processed one time, two times, or ten times must produce
the same final business outcome.

## 2. The real-world problem (why this matters)

Imagine you work at a bank. A payments gateway sends you a credit for
₹500 to account `ACC-203`. The network blips. The gateway doesn't get an
acknowledgement, so it retries. The retry succeeds and your service posts
₹500 to the account. Then the first request (which actually did arrive
but whose ack was lost) finally gets re-driven by the gateway and comes
through — again. Without protection, you'd credit the account **twice**.

Now add two more real-world facts:
- A single business transaction may generate many request events (a
  CREDIT, a REVERSAL, a correction), and these can arrive in **any order**.
- Downstream systems (the ledger database, a message broker, an API)
  **sometimes fail transiently** — the right response is to retry, but
  retrying blindly means duplicate posts.

The whole point of this service is to solve those three problems at
once: duplicate detection, out-of-order handling, and safe retry.

## 3. Architecture at 10,000 feet

```
                 ┌─────────────────────┐
    JSON file ─▶ │ TransactionSource   │   (interface; swap for Kafka,
                 └─────────────────────┘    SQS, HTTP, DB-poller…)
                           │
                           ▼
                 ┌─────────────────────┐
                 │ BackgroundIngestor  │   loops through the source,
                 │                     │   submits each record to the
                 │                     │   processor, then calls
                 │                     │   drainPending() at end.
                 └─────────────────────┘
                           │
                           ▼
     ┌───────────────────────────────────────────────────────────────┐
     │                      TransactionProcessor                     │
     │  Stage 1  validate (TransactionValidator)                     │
     │  Stage 2  dedupe by transactionId (TransactionStateStore)     │
     │  Stage 3  dedupe by requestId (IdempotencyStore)              │
     │  Stage 4  order check (AccountSequenceTracker, per account)   │
     │  Stage 5  apply with retry (LedgerStore + RetryPolicy +       │
     │                              FailureSimulator)                │
     └───────────────────────────────────────────────────────────────┘
                           │
                           ▼
                 ┌─────────────────────┐
                 │  LedgerStore        │   final "money moves here"
                 │  (balances + guard) │   step; the guard makes the
                 │                     │   same transactionId a no-op
                 │                     │   on a second apply.
                 └─────────────────────┘
                           │
                           ▼
                 ┌─────────────────────┐
                 │ ProcessingReport    │   counters per status,
                 │ ReportPrinter       │   end-of-batch summary.
                 └─────────────────────┘
```

Four things to notice:
1. The **processor depends on interfaces**, not concrete classes. Swap
   the file source for Kafka without touching the business logic.
2. Each stage has one job. If any stage rejects, the request short-circuits
   with a clear status (`FAILED`, `DUPLICATE`, `PENDING_ORDER`).
3. The ledger has its **own** idempotency guard. The processor could call
   `apply` five times for the same transactionId and only the first one
   would move money.
4. Nothing is stateful at the module boundary — all state lives in
   explicit stores, which can be swapped for a database.

## 4. The 5-stage pipeline — one transaction's journey

Let's follow one request from arrival to final state.

```
   TransactionRequest arrives
            │
            ▼
   ┌──────────────────────┐
   │ 1. Validate          │  Valid? ────no───▶ status = FAILED (reason)
   │                      │                     stop.
   │  TransactionValidator│
   └──────────────────────┘
            │ yes
            ▼
   ┌──────────────────────┐
   │ 2. Check             │  Found a record    status = DUPLICATE
   │    transactionId     │  in the state ──▶  stop.
   │                      │  store?
   │  TransactionStateStore
   └──────────────────────┘
            │ no
            ▼
   ┌──────────────────────┐
   │ 3. markSeen(reqId)   │  Already seen? ──▶ status = DUPLICATE
   │  (atomic SETNX-ish)  │                    stop.
   │  IdempotencyStore    │
   └──────────────────────┘
            │ first time
            ▼
   ┌──────────────────────┐
   │ 4. Order gate        │  seq > expected?  park in buffer
   │                      │  seq == expected?  release + drain any
   │ AccountSequenceTrack │                    newly-contiguous ones
   │                      │  seq <  expected?  pass through
   └──────────────────────┘  (upstream dedupe
            │                  catches it)
            ▼ ready
   ┌──────────────────────┐
   │ 5. Apply with retry  │  LedgerStore.apply()
   │                      │     ├── transient fail? retry with backoff
   │ TransactionProcessor │     │                   (max N attempts)
   │ RetryPolicy          │     └── success? status = PROCESSED
   │ LedgerStore          │
   └──────────────────────┘
```

That's the whole runtime path. Everything else is scaffolding.

## 5. Component tour — what each piece owns and depends on

Grouped by package so you can navigate the code while reading.

### 5.1 `model/` — the data

| Class | What it holds | Mutability |
|-------|---------------|------------|
| `TransactionRequest` | Immutable inbound record: `transactionId`, `requestId`, `accountId`, `transactionType`, `amount`, `currency`, `sequenceNumber`, `eventTime`. | **Immutable** (Java record). |
| `TransactionType` | enum: `CREDIT`, `DEBIT`, `REVERSAL`. | — |
| `TransactionStatus` | enum: `RECEIVED`, `PROCESSING`, `PROCESSED`, `DUPLICATE`, `FAILED`, `RETRY_PENDING`, `PENDING_ORDER`. | — |
| `TransactionRecord` | Mutable wrapper around a `TransactionRequest`. Tracks `status`, `attemptCount`, `failureReason`, `lastUpdatedAt`. | **Mutable** but thread-safe (synchronized setters, volatile fields). |

Dependency rule: the model layer depends on **nothing else** in the project.

### 5.2 `validator/` — the gatekeeper

| Class | Role |
|-------|------|
| `TransactionValidator` | Returns `ValidationResult.ok()` or `ValidationResult.invalid(reason)`. Checks: non-null fields, positive amount, supported currency, non-negative sequence, amount < 1 billion. |
| `ValidationResult` | Immutable `(valid, reason)` pair. |

Why it's its own package: validation is a **boundary concern**. Once a
request passes the validator, the rest of the code assumes well-formed
input and does not re-check.

Depends on: `model/` only.

### 5.3 `store/` — the memory

Three stores, each behind an interface. Why? So we can swap in-memory for
a database (Postgres, Redis) without touching the business logic.

| Interface | Role | In-memory impl |
|-----------|------|----------------|
| `IdempotencyStore` | **Atomic SETNX** on `requestId`. `markSeen(k)` returns `true` only to the winning caller; everyone else gets `false`. | `ConcurrentHashMap.putIfAbsent` under the hood. |
| `TransactionStateStore` | Keyed by `transactionId`. Holds the mutable `TransactionRecord`. | `ConcurrentHashMap<String, TransactionRecord>`. |
| `LedgerStore` | The place money actually moves. `apply(request)` is **idempotent on `transactionId`**: a second call for the same id returns `false` and does nothing. | `ConcurrentHashMap<String, BigDecimal>` for balances + a `Set<String>` of applied txn ids, protected by a monitor. |

Depends on: `model/` only.

### 5.4 `ordering/` — the traffic controller

| Class | Role |
|-------|------|
| `AccountSequenceTracker` | Per-account strict-forward sequence tracker. Each account has an "expected next sequence" starting at 1. Arrivals that match → release. Higher → park in a `TreeMap<Long, TransactionRecord>`. Lower → return (upstream dedupe handles them). |

Core methods:
- `accept(record)` → `List<TransactionRecord>` of records now ready to process.
- `drainAll()` → releases every parked record at end-of-batch so an
  account whose first event is at `seq > 1` doesn't stay stuck forever.

Depends on: `model/` only. (No store, no processor — it's a pure data
structure with per-account state.)

### 5.5 `retry/` — the resilience

| Class | Role |
|-------|------|
| `RetryPolicy(maxAttempts, baseBackoffMillis)` | Pure data: tells callers whether to retry and how long to back off. Linear backoff in this build; swap for exponential + jitter in production. |
| `TransientProcessingException` | Marker thrown by code that wants to signal "retry me". Any other exception is terminal. |
| `FailureSimulator` | **Test aid only.** For a configured set of transactionIds, throws `TransientProcessingException` on the first attempt and succeeds afterwards. Lets us demonstrate retry without a real downstream to flake. |

Depends on: `model/` only.

### 5.6 `processor/` — the brain

| Class | Role |
|-------|------|
| `TransactionProcessor` | Runs the 5-stage pipeline. All its collaborators arrive by constructor; it owns no infrastructure itself. |

The one method that drives everything is `submit(TransactionRequest r)`.
There's also `drainPending()`, called by the ingestor at end-of-batch.

Depends on: `model/`, `validator/`, `store/`, `ordering/`, `retry/`, `report/`.
This is the one place where all the pieces come together.

### 5.7 `ingestion/` — the mouth

| Class | Role |
|-------|------|
| `TransactionSource` (interface) | `Iterable<TransactionRequest> read()`. |
| `JsonFileTransactionSource` | Reads a JSON array from the classpath or filesystem. |
| `BackgroundIngestor` | For each record from the source, calls `processor.submit(r)`, with try/catch so one bad record never halts the batch. Finally calls `processor.drainPending()`. |

Depends on: `model/`, `processor/`.

### 5.8 `report/` — the eyes

| Class | Role |
|-------|------|
| `ProcessingReport` | Thread-safe counters per `TransactionStatus`, plus total retries and total submissions. |
| `ReportPrinter` | Prints the end-of-batch summary: counts per status, retry total, per-record final state, ledger balances. |

Depends on: `model/`, `store/`.

### 5.9 `config/` — knobs

| Class | Role |
|-------|------|
| `ProcessorConfig` | Immutable record of `startingSequence`, `maxAttempts`, `baseBackoffMillis`, `transientFailureTxnIds` (for the simulator), `inputResource`. One place to tune everything the CLI runs with. |

### 5.10 Entry point

`TransactionProcessorApplication.main(args)` is the CLI. It:
1. Reads the config.
2. Wires every component manually (constructor injection — no framework).
3. Runs one batch.
4. Prints the summary.

That manual wiring is deliberate: it makes the dependency graph **readable
on one screen**. You can see exactly who gets what.

## 6. The four safety nets — the heart of the design

Why four, not one? Because any single layer has a failure mode. Stacking
four means we're still correct even if upstream dedupe is bypassed, even
if the service crashes mid-apply, and even if the client regenerates its
idempotency key.

```
         Outer                                    Inner
         ─────                                    ─────
   ┌──────────┐   ┌──────────────┐   ┌──────────────┐   ┌────────────┐
   │ 1. Boun- │   │ 2. txnId     │   │ 3. requestId │   │ 4. Ledger  │
   │ dary     │   │ state check  │   │ atomic       │   │ idempotency│
   │ validate │   │              │   │ markSeen     │   │ guard      │
   └──────────┘   └──────────────┘   └──────────────┘   └────────────┘

   Rejects       Catches same      Catches client-   Final defence:
   malformed     business txn      side retries      a second apply()
   input before  with a new        with same         with the same
   any state is  idempotency       requestId.        transactionId
   touched.      key.                                is a no-op.
```

**Thought experiment — what breaks if we remove each guard:**

- Remove **(1) Validation** → a `null` field crashes the processor
  mid-batch; the batch halts; the operator has to clean up by hand.
- Remove **(2) transactionId state check** → a client that regenerates
  its idempotency key for the same business transaction gets charged
  twice. (Real world: clients do this.)
- Remove **(3) requestId markSeen** → a client that times out and
  retries with the same key double-posts. (Real world: this is the
  common case.)
- Remove **(4) Ledger guard** → a crash **between** the ledger write and
  the processor marking the record `PROCESSED` means the recovery/replay
  will apply the same transaction again. (Real world: ack-lost scenarios.)

The test `RetryTest.innerLedgerIdempotency_preventsDoubleApplyAcrossRetries`
exercises the guard-4 case specifically: the ledger succeeds, "ack is
lost", the processor retries, and the balance stays correct.

## 7. Out-of-order handling — exactly how it works

**Strategy chosen:** Option A in the brief — per-account strict-forward
sequence (`AccountSequenceTracker`).

**Mental model:** each account has a little bouncer at the door. The
bouncer has a clipboard that says "the next sequence number I'm expecting
is N". When a transaction arrives:

- If its seq == N → let it in; increment N; see if any transactions in
  the holding area now match the new N and let them in too.
- If its seq > N → send it to the holding area, sorted by seq.
- If its seq < N → send it to the processor; dedupe will typically catch
  it as a duplicate.

**Walked example (the §9.8 sample data):**

| Arrival | txId     | req       | acct    | seq | What the tracker does                                      |
|---------|----------|-----------|---------|-----|------------------------------------------------------------|
| 1       | TXN-2002 | REQ-2     | ACC-201 | 2   | expected=1, 2>1 → park. Status = PENDING_ORDER.            |
| 2       | TXN-2001 | REQ-1     | ACC-201 | 1   | matches → release. Drain: 2 now contiguous → release. Both PROCESSED. |
| 3       | TXN-2002 | REQ-2-DUP | ACC-201 | 2   | stateStore says TXN-2002 is already PROCESSED → DUPLICATE. |
| 4       | TXN-2003 | REQ-3     | ACC-201 | 3   | expected=3, matches → PROCESSED.                           |
| 5       | TXN-2004 | REQ-4     | ACC-202 | 4   | amount is −10, FAILS validation. Status = FAILED.          |
| 6       | TXN-2005 | REQ-5     | ACC-203 | 5   | expected=1 for ACC-203, 5>1 → park. End of batch.          |
| drain   | TXN-2005 | REQ-5     | ACC-203 | 5   | drainPending() releases it → simulator throws → retry → PROCESSED on attempt 2. |

Final balances: `ACC-201 = 100 - 40 + 25 = 85`, `ACC-203 = 500`.
Status counts: 4 PROCESSED, 1 DUPLICATE, 1 FAILED, 1 retry.

**Trade-off I accepted:** an account whose first-observed event has
`seq > 1` only releases on `drainPending()`. For a batch service that's
fine. For a true streaming service you'd want a **time-based grace
window** (e.g. "release the oldest if no new arrivals for 10 s"). The
hook is one method on the tracker.

## 8. Retries — why they're safe

Two kinds of failure, cleanly split:

- `TransientProcessingException` → retry up to `maxAttempts` with
  bounded backoff.
- Any other exception → terminal `FAILED`, no retry.

The subtle question: **what if the ledger succeeds but the retry still
fires?** (Classic "ack lost after apply" scenario — see
`RetryTest.innerLedgerIdempotency_preventsDoubleApplyAcrossRetries`.)

The ledger's own guard handles it:
```
apply(request):
    if appliedTransactionIds.add(request.transactionId):
        actually move money
        return true
    else:
        return false     // already applied, do nothing
```

Because the guard uses the same `transactionId` across every attempt,
the second apply is a safe no-op. The processor sees `apply` return
`false`, logs it, and still marks the record `PROCESSED`. The business
outcome is correct.

## 9. End-to-end example (walking the sample data through the system)

Here's the state after each arrival. **S** = state store, **L** = ledger.

```
Start:  S = {}                      L = {}                       pending = {}

Arr 1   TXN-2002 (seq 2) ACC-201 arrives.
        Validate OK. S.find(TXN-2002) = nothing. markSeen(REQ-2) = true.
        Tracker: expected[ACC-201]=1, 2>1, park.
        S = {TXN-2002: PENDING_ORDER}

Arr 2   TXN-2001 (seq 1) ACC-201 arrives.
        Validate OK. markSeen(REQ-1) = true.
        Tracker: 1 matches. Release. Drain → 2 contiguous. Release.
        Process TXN-2001: credit 100 → L[ACC-201] = 100. PROCESSED.
        Process TXN-2002: debit 40 → L[ACC-201] = 60. PROCESSED.
        S = {TXN-2001: PROCESSED, TXN-2002: PROCESSED}

Arr 3   TXN-2002 (seq 2) ACC-201 arrives (REQ-2-DUP).
        Validate OK. S.find(TXN-2002) = PROCESSED → DUPLICATE. Stop.
        report.duplicate = 1

Arr 4   TXN-2003 (seq 3) ACC-201 arrives.
        Validate OK. markSeen(REQ-3) = true.
        Tracker: expected[ACC-201]=3, 3 matches. Release.
        Credit 25 → L[ACC-201] = 85. PROCESSED.

Arr 5   TXN-2004 (seq 4) ACC-202 arrives.
        Validate: amount is -10 → INVALID. Mark FAILED, don't touch tracker.
        S = {..., TXN-2004: FAILED (reason: 'amount must be positive')}

Arr 6   TXN-2005 (seq 5) ACC-203 arrives.
        Validate OK. markSeen(REQ-5) = true.
        Tracker: expected[ACC-203]=1, 5>1, park.
        S = {..., TXN-2005: PENDING_ORDER}

Drain   TXN-2005 is drained from the tracker.
        processWithRetry(TXN-2005):
            attempt 1: simulator throws transient → RETRY_PENDING, backoff
            attempt 2: ledger.apply succeeds → L[ACC-203] = 500. PROCESSED.

Done.   L = {ACC-201: 85, ACC-203: 500}
        Summary: processed=4 duplicate=1 failed=1 retries=1
```

The `BatchIngestionTest` asserts exactly these numbers.

## 10. Dependency diagram (who calls what)

```
TransactionProcessorApplication (main)
          │
          │  wires everything, then →
          ▼
BackgroundIngestor ──▶ JsonFileTransactionSource
          │
          │  for each request →
          ▼
TransactionProcessor
    ├──▶ TransactionValidator
    ├──▶ IdempotencyStore      (interface; InMemoryIdempotencyStore)
    ├──▶ TransactionStateStore (interface; InMemoryTransactionStateStore)
    ├──▶ LedgerStore           (interface; InMemoryLedgerStore)
    ├──▶ AccountSequenceTracker
    ├──▶ RetryPolicy
    ├──▶ FailureSimulator
    └──▶ ProcessingReport

ReportPrinter (end-of-batch) ──▶ ProcessingReport
                              └▶ TransactionStateStore
                              └▶ InMemoryLedgerStore
```

**Everything above a dotted boundary depends only on interfaces.** That's
why `TransactionProcessor` is testable without any file I/O, and why
production can swap each store for Postgres/Redis later without changing
the brain.

## 11. How we built this (process notes)

In the order the work actually happened:

1. **Read the brief twice.** Identified the four invariants that had to
   hold: idempotency, out-of-order safety, retry safety, invalid-record
   isolation.
2. **Picked the stack.** Java 17 + Maven, no Spring. Reasoning: the brief
   says "no complex UI" and "no production DB required" — Spring Boot
   adds surface area a reviewer has to parse through to find the business
   logic. Plain Maven keeps the signal-to-noise high.
3. **Modeled the domain first.** `TransactionRequest`, enums, `TransactionRecord`.
   Decided on records (immutable) for the request and a mutable wrapper
   for the processing state. This encoded the invariant "input is
   immutable, processing state evolves" at the type level.
4. **Wrote the stores as interfaces** before writing any implementation.
   The in-memory impls followed as the simplest thing that passes tests.
5. **Wrote the validator.** Rejected everything the brief listed and a
   few more (unsupported currency, max amount).
6. **Wrote `AccountSequenceTracker`.** This is the trickiest piece — I
   went through three variants:
   - **Variant 1 (rejected):** per-account `startingSeq = min seen`.
     Would process TXN-2002 (seq 2) immediately when it arrived first,
     then reject TXN-2001 (seq 1) as "late". That contradicts the brief.
   - **Variant 2 (rejected):** no sequence check, apply by state only.
     Fine for a flat ledger, but didn't demonstrate out-of-order handling.
   - **Variant 3 (kept):** per-account `startingSeq = 1`, park futures,
     drain at end-of-batch. Matches the brief exactly, trade-off is
     documented in DESIGN.md.
7. **Wrote the retry layer.** Chose `TransientProcessingException` as the
   marker so we don't accidentally retry a `NullPointerException`.
8. **Wrote the processor.** Composed everything above. Added the four
   safety nets intentionally, in order.
9. **Wrote the ingestor and the JSON source.** The source reads from
   classpath first so the sample ships inside the jar.
10. **Wrote tests to challenge each decision.** Five test classes, 14
    tests. The "ack lost after apply" test was the one that forced the
    inner ledger guard to be rock-solid.
11. **Compiled and fixed small issues** — one accessor name collision
    on `ValidationResult.valid()` (the record's auto-generated accessor
    collided with the static factory I wanted to call `valid()` — renamed
    the factory to `ok()`).
12. **Wrote the design notes and this walkthrough.**

## 12. Interview questions you'll be asked — and how to answer

**Q1. Walk me through what this service does.**
> "It's a background job that processes transaction requests coming from
> upstream systems. The requests can be duplicated, arrive out of order,
> or fail transiently. The service guarantees idempotent processing — the
> same request, replayed any number of times, produces the same final
> balance — through four stacked safety guards. It has a per-account
> sequence tracker for out-of-order handling and a bounded retry loop for
> transient failures. The core processor is pure business logic; sources
> and stores are behind interfaces so we can later swap the file source
> for a Kafka consumer and the in-memory ledger for a Postgres table."

**Q2. What is idempotency and why is it the headline requirement?**
> "Idempotent means 'the same operation performed many times has the
> same effect as doing it once'. For money, this is non-negotiable:
> network retries, client bugs, and crash-replays will absolutely cause
> the same request to arrive multiple times. If the service isn't
> idempotent, people get charged twice. Everything in the architecture —
> the dedupe keys, the ledger guard, the retry policy — serves this one
> invariant."

**Q3. Why four idempotency layers? Isn't one enough?**
> "Each layer defends against a different failure mode. The validator
> keeps malformed input out. The transactionId state check catches a
> client that regenerated its idempotency key for the same business
> transaction. The atomic markSeen catches a client-side retry with the
> same key. The ledger guard catches the hardest case — a crash between
> the ledger write and the record being marked PROCESSED. Any one layer
> missing exposes a specific real-world failure. A test exercises each."

**Q4. What's the difference between transactionId and requestId?**
> "`transactionId` is the business key — it identifies the real-world
> transaction. `requestId` is the idempotency key — it identifies a
> particular HTTP/message delivery of that transaction. A single
> `transactionId` could be sent many times with different `requestId`s
> if the client has a bug; we still have to treat the second one as a
> duplicate, which is why both checks exist."

**Q5. How does your out-of-order handling work?**
> "Per-account strict-forward sequence. Each account has a 'next
> expected sequence' counter. Arrivals that match get released. Higher
> ones get parked in a sorted buffer. On every match, I drain the buffer
> for the newly-contiguous records. For an account whose first observed
> event has a sequence greater than 1 — like ACC-203 at seq 5 — the
> buffer gets drained at end-of-batch. In production I'd replace the
> end-of-batch drain with a time-based grace window and a dead-letter
> queue for anything still waiting."

**Q6. Why Option A (sequence) over Option B (state machine) or Option C (latest-state)?**
> "Option B is strongest for domains with a real lifecycle, like
> `CREATED → AUTHORIZED → CAPTURED`. A flat ledger with CREDIT/DEBIT/
> REVERSAL doesn't really have a transition table — the state is just
> the running balance. Option C would silently drop late events, which
> is wrong for money. Option A gives me a crisp testable invariant:
> processed sequences per account form a contiguous range."

**Q7. What happens if the service crashes between the ledger write and marking PROCESSED?**
> "On restart the record is still RECEIVED or PROCESSING, so the replay
> re-enters the apply path. The ledger's internal idempotency set already
> contains that `transactionId`, so `apply` returns false and does
> nothing. The processor logs 'ledger already applied' and marks the
> record PROCESSED. Zero duplicate money movement. There's a test for
> exactly this — `innerLedgerIdempotency_preventsDoubleApplyAcrossRetries`."

**Q8. What if transient failures keep happening — do we lose the transaction?**
> "No. The retry policy is bounded — three attempts in this build. After
> the third failure the record moves to `FAILED` with the last failure
> reason recorded. It's visible in the end-of-batch summary and in the
> per-record dump. In production I'd route exhausted records to a DLQ
> for human triage; the structure is already there."

**Q9. Why no Spring Boot?**
> "The brief said no complex UI and no mandatory DB. Spring Boot would
> have added a lot of surface area — a reviewer would spend time on
> autoconfig and starters before finding the business logic. Plain Java
> + Maven keeps the signal high. If this went to production, the
> composition root (TransactionProcessorApplication) is the only file
> that would need to change to wire Spring in."

**Q10. How did AI help you build this? Where did you push back?**
> "I used it as a pair-programmer, one module at a time, with the
> invariants quoted explicitly in each prompt — the four idempotency
> guards, the ordering trade-off, no-PII logs. Where I pushed back:
> I rejected an initial Spring Boot scaffolding (overkill), an
> overeager state-machine design (wrong for a flat ledger), and a
> first-cut tracker that would eagerly release the first-seen sequence
> (would have mis-processed TXN-2001 as late in the brief's test case).
> I validated every piece by writing tests before accepting the design —
> the 'ack lost after apply' test forced tightening the inner ledger
> guard."

**Q11. What would you change for production?**
> Six-item list from `DESIGN.md §9`, delivered in order of risk-reduction:
> 1. Durable Postgres-backed state + idempotency + ledger with a
>    `UNIQUE(transaction_id)` constraint as the inner guard.
> 2. Real queue adapter (Kafka/SQS) behind `TransactionSource`.
> 3. Dead-letter queue for stuck pending-order and exhausted retries.
> 4. Time-based grace window in the ordering tracker.
> 5. Prometheus metrics + Grafana dashboard + alerts on failure rate and DLQ depth.
> 6. Per-account worker pool so a slow account can't starve others.

**Q12. How did you test it?**
> "Five test classes, 14 tests:
> - `DuplicateDetectionTest` — requestId dedupe, transactionId dedupe, no
>   double posts.
> - `OutOfOrderTest` — parked and drained in order, independence across
>   accounts, drain-at-end for first-event-high-seq accounts, plus the
>   full §9.8 sample as one assertion.
> - `ValidationTest` — invalid amount, malformed records don't halt the
>   batch, unsupported currency.
> - `RetryTest` — transient failure retried and succeeds, retries
>   exhaust to FAILED, inner idempotency preserved across retries.
> - `BatchIngestionTest` — end-to-end over the bundled JSON, asserts
>   exact balances and counts."

## 13. One-line explanations you can memorize

- **Idempotency** — same request processed many times = same final outcome.
- **transactionId** — the business key.
- **requestId** — the idempotency key.
- **Four guards** — validate → state → markSeen → ledger.
- **Option A** — per-account strict-forward sequence, buffer the futures.
- **Retry** — bounded, transient-only, inner-guarded.
- **Composition root** — the main class is the only wiring; everything
  else depends only on interfaces.

That's the whole system in seven sentences.
