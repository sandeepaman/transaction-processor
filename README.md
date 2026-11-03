# Idempotent Background Transaction Processor

FDE L2 practical assessment — a production-aware background service that
processes transaction requests arriving in any order, with duplicates, with
transient failures, without ever double-posting to the ledger.

See [`DESIGN.md`](DESIGN.md) for architecture, trade-offs, AI-assisted
development notes, and production-hardening recommendations.

## Quick start

```bash
# Requires Java 17+ and Maven 3.9+.
mvn test                     # runs 14 unit/integration tests
mvn -DskipTests package      # builds the executable jar
java -jar target/transaction-processor.jar
```

The default run processes [`sample-transactions.json`](src/main/resources/sample-transactions.json),
which is the exact test-data table from §9.8 of the brief. To process your
own file:

```bash
java -jar target/transaction-processor.jar /absolute/path/to/your-input.json
```

Any argument is resolved as a classpath resource first, then as a filesystem
path, so you can also drop a file onto the classpath instead.

## What the sample run proves

Running the bundled sample produces this summary (verified by
[`BatchIngestionTest`](src/test/java/com/fde/txnprocessor/BatchIngestionTest.java)
and reproducible by the command above):

```
Requests submitted : 6
  PROCESSED   = 4
  DUPLICATE   = 1
  FAILED      = 1
Retry attempts     : 1

tx=TXN-2001 PROCESSED attempts=1       (CREDIT 100 to ACC-201)
tx=TXN-2002 PROCESSED attempts=1       (DEBIT  40, arrived out-of-order, drained after TXN-2001)
tx=TXN-2003 PROCESSED attempts=1       (CREDIT 25)
tx=TXN-2004 FAILED    reason=amount must be positive
tx=TXN-2005 PROCESSED attempts=2       (retried once after simulated transient failure)

ACC-201 balance = 85
ACC-203 balance = 500
```

The DUPLICATE count (`REQ-2-DUP` replaying `TXN-2002`) is tracked by the
`ProcessingReport` even though the transaction-state store is keyed by
transactionId and so holds only one record per transaction.

## Repository layout

```
src/main/java/com/fde/txnprocessor/
  TransactionProcessorApplication.java  — CLI entry point: wires components, runs one batch.
  model/                                — Immutable request + mutable record + status/type enums.
  validator/                            — Boundary input validation; rejects malformed records.
  store/                                — Idempotency, transaction-state, and ledger stores (interfaces + in-memory impls).
  ordering/                             — Per-account AccountSequenceTracker (Option A: sequence-based).
  retry/                                — RetryPolicy, FailureSimulator, TransientProcessingException.
  processor/                            — TransactionProcessor: validate → dedupe → order → apply with retry.
  ingestion/                            — TransactionSource + JsonFileTransactionSource + BackgroundIngestor.
  report/                               — ProcessingReport + ReportPrinter.
  config/                               — ProcessorConfig (static defaults).

src/main/resources/
  sample-transactions.json              — Bundled §9.8 test table.
  logback.xml                           — Structured console logs.

src/test/java/com/fde/txnprocessor/
  TestFixtures.java                     — Shared test wiring.
  DuplicateDetectionTest.java           — Request-level + transaction-level dedupe.
  OutOfOrderTest.java                   — Per-account sequence tracker, drain-at-end.
  ValidationTest.java                   — Malformed/invalid records are isolated.
  RetryTest.java                        — Retry succeeds, exhaustion fails, inner idempotency.
  BatchIngestionTest.java               — End-to-end batch over the bundled sample.
```

## Design highlights

- **Four stacked idempotency guards**: boundary validation → transactionId
  state check → atomic requestId `markSeen` → inner ledger idempotency
  (second apply of same transactionId is a no-op). Even a replay after a
  crash between ledger write and ack cannot double-post.
- **State-based + sequence ordering**: per-account `AccountSequenceTracker`
  parks higher sequences until predecessors arrive; drain-at-end-of-batch
  releases accounts whose first observed event is at `seq > starting`.
- **Transient vs permanent failures split cleanly**: only
  `TransientProcessingException` is retried; everything else is terminal
  immediately. `FailureSimulator` exists only to exercise the retry path
  deterministically in tests and in the sample run.
- **Infrastructure-free core**: `TransactionProcessor` depends on interfaces
  only, so the same class runs under the CLI, under JUnit, and under any
  future queue/HTTP/DB adapter without modification.
- **No PII in logs**: structured logs carry transactionId, requestId,
  accountId, status, attempt — never amount or currency.

## Explicit non-goals (per §9.3 Out of Scope)

- No real queue or distributed-transaction integration — a `TransactionSource`
  adapter would plug into Kafka/JMS/SQS/HTTP without touching the processor.
- No persistence — stores are in-memory; swap for a Postgres/Redis-backed
  implementation of the same three interfaces.
- No UI, no multi-region deployment, no full observability stack.

## FR coverage map

| ID       | Covered by                                                                 |
|----------|----------------------------------------------------------------------------|
| L2-FR01  | `TransactionSource` + `JsonFileTransactionSource` (classpath + file paths) |
| L2-FR02  | `TransactionValidator` + `ValidationTest`                                  |
| L2-FR03  | `TransactionStateStore` / `InMemoryTransactionStateStore`                  |
| L2-FR04  | `TransactionStatus` enum + transitions in `TransactionProcessor`           |
| L2-FR05  | txnId check + atomic `markSeen` + inner ledger guard (`LedgerStore.apply`) |
| L2-FR06  | `DuplicateDetectionTest`, `OutOfOrderTest` (REQ-2-DUP case)                |
| L2-FR07  | `AccountSequenceTracker` (Option A, documented in DESIGN.md §4)            |
| L2-FR08  | `FailureSimulator` + `RetryTest.transientFailure...`                       |
| L2-FR09  | `RetryTest.innerLedgerIdempotency_preventsDoubleApplyAcrossRetries`        |
| L2-FR10  | Package layout: `validator`, `ordering`, `processor` separate from `ingestion` |
| L2-FR11  | `ProcessingReport` + `ReportPrinter`                                       |
| L2-FR12  | Five test classes, 14 tests covering all four scenario families           |
