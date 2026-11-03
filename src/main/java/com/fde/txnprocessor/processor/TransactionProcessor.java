package com.fde.txnprocessor.processor;

import com.fde.txnprocessor.model.TransactionRecord;
import com.fde.txnprocessor.model.TransactionRequest;
import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.ordering.AccountSequenceTracker;
import com.fde.txnprocessor.report.ProcessingReport;
import com.fde.txnprocessor.retry.FailureSimulator;
import com.fde.txnprocessor.retry.RetryPolicy;
import com.fde.txnprocessor.retry.TransientProcessingException;
import com.fde.txnprocessor.store.IdempotencyStore;
import com.fde.txnprocessor.store.LedgerStore;
import com.fde.txnprocessor.store.TransactionStateStore;
import com.fde.txnprocessor.validator.TransactionValidator;
import com.fde.txnprocessor.validator.ValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

/**
 * Core business pipeline. Pure, infrastructure-free: all inputs (sources,
 * stores, policies, simulators) arrive via constructor so the same class
 * runs under the CLI ingestor and under JUnit without changes.
 *
 * Pipeline per submitted request:
 *   validate -> dedupe (transactionId + requestId) -> order (per-account) ->
 *   apply with retry.
 *
 * Four safety nets stacked, outer to inner:
 *   1. TransactionValidator       — rejects malformed input at the boundary.
 *   2. stateStore by transactionId — catches a replay of the same business tx
 *                                    even if the client regenerated requestId.
 *   3. idempotencyStore by requestId — atomic SETNX-style gate against client retries.
 *   4. ledgerStore.apply           — inner guard: a second apply of the same
 *                                    transactionId is a no-op, so a crash-and-replay
 *                                    at the step 4 boundary still cannot double-post.
 *
 * Observability: every meaningful state change is a structured log line with
 * transactionId, requestId, account, status, attempt — correlation-friendly
 * without ever logging amount or currency (see SecurityNotes in DESIGN.md).
 */
public class TransactionProcessor {

    private static final Logger log = LoggerFactory.getLogger(TransactionProcessor.class);

    private final TransactionValidator validator;
    private final IdempotencyStore idempotencyStore;
    private final TransactionStateStore stateStore;
    private final LedgerStore ledgerStore;
    private final AccountSequenceTracker sequenceTracker;
    private final RetryPolicy retryPolicy;
    private final FailureSimulator failureSimulator;
    private final ProcessingReport report;

    public TransactionProcessor(TransactionValidator validator,
                                IdempotencyStore idempotencyStore,
                                TransactionStateStore stateStore,
                                LedgerStore ledgerStore,
                                AccountSequenceTracker sequenceTracker,
                                RetryPolicy retryPolicy,
                                FailureSimulator failureSimulator,
                                ProcessingReport report) {
        this.validator = validator;
        this.idempotencyStore = idempotencyStore;
        this.stateStore = stateStore;
        this.ledgerStore = ledgerStore;
        this.sequenceTracker = sequenceTracker;
        this.retryPolicy = retryPolicy;
        this.failureSimulator = failureSimulator;
        this.report = report;
    }

    public void submit(TransactionRequest request) {
        report.onSubmitted();
        String correlationId = request == null || request.requestId() == null
                ? "req-" + UUID.randomUUID()
                : request.requestId();

        // Step 1: validate.
        ValidationResult v = validator.validate(request);
        if (!v.valid()) {
            handleInvalid(request, v.reason(), correlationId);
            return;
        }

        // Step 2: transactionId-level dedupe. If we've seen this business
        // transaction before in ANY state, treat as duplicate — only one
        // flight per transactionId is allowed through the pipeline.
        var existing = stateStore.find(request.transactionId());
        if (existing.isPresent()) {
            markDuplicate(request, existing.get(), correlationId);
            return;
        }

        // Step 3: requestId-level atomic dedupe. markSeen returns true for the
        // single winner; losers short-circuit as duplicate. This is the gate
        // that protects us from client-side retries that use the same key.
        if (!idempotencyStore.markSeen(request.requestId())) {
            log.info("Duplicate requestId: tx={} req={} account={}",
                    request.transactionId(), request.requestId(), request.accountId());
            report.onTerminal(TransactionStatus.DUPLICATE);
            return;
        }

        TransactionRecord fresh = new TransactionRecord(request);
        TransactionRecord stored = stateStore.putIfAbsent(fresh);
        if (stored != fresh) {
            markDuplicate(request, stored, correlationId);
            return;
        }

        log.info("Received: tx={} req={} account={} seq={} type={}",
                request.transactionId(), request.requestId(), request.accountId(),
                request.sequenceNumber(), request.transactionType());

        // Step 4: ordering gate.
        List<TransactionRecord> ready = sequenceTracker.accept(fresh);
        if (ready.isEmpty()) {
            fresh.setStatus(TransactionStatus.PENDING_ORDER);
            return;
        }
        for (TransactionRecord ready1 : ready) {
            processWithRetry(ready1);
        }
    }

    /**
     * Called by the ingestor at end-of-batch. Releases any records still
     * parked by the sequence tracker and runs them through processing.
     * Keeps the "no stuck records on happy path" promise visible in tests.
     */
    public void drainPending() {
        List<TransactionRecord> drained = sequenceTracker.drainAll();
        if (drained.isEmpty()) {
            return;
        }
        log.info("Draining {} pending-order record(s) at end of batch", drained.size());
        for (TransactionRecord r : drained) {
            processWithRetry(r);
        }
    }

    private void processWithRetry(TransactionRecord record) {
        TransactionRequest r = record.request();
        while (true) {
            record.incrementAttempt();
            record.setStatus(TransactionStatus.PROCESSING);
            try {
                failureSimulator.maybeFail(r);
                boolean applied = ledgerStore.apply(r);
                if (!applied) {
                    // Ledger guard said "already done". That is still a success
                    // from the caller's perspective — the business outcome holds.
                    log.info("Ledger already applied (inner idempotency): tx={} req={}",
                            r.transactionId(), r.requestId());
                }
                record.setStatus(TransactionStatus.PROCESSED);
                report.onTerminal(TransactionStatus.PROCESSED);
                log.info("Processed: tx={} req={} account={} seq={} attempts={}",
                        r.transactionId(), r.requestId(), r.accountId(),
                        r.sequenceNumber(), record.attemptCount());
                return;
            } catch (TransientProcessingException e) {
                record.setFailureReason(e.getMessage());
                report.onRetryAttempt();
                if (!retryPolicy.shouldRetry(record.attemptCount())) {
                    record.setStatus(TransactionStatus.FAILED);
                    report.onTerminal(TransactionStatus.FAILED);
                    log.error("Max retries exhausted: tx={} req={} attempts={} reason={}",
                            r.transactionId(), r.requestId(), record.attemptCount(), e.getMessage());
                    return;
                }
                record.setStatus(TransactionStatus.RETRY_PENDING);
                long backoff = retryPolicy.backoffMillis(record.attemptCount());
                log.warn("Transient failure, retrying: tx={} req={} attempt={} backoffMs={} reason={}",
                        r.transactionId(), r.requestId(), record.attemptCount(), backoff, e.getMessage());
                sleepQuietly(backoff);
            } catch (RuntimeException e) {
                record.setStatus(TransactionStatus.FAILED);
                record.setFailureReason(e.getMessage());
                report.onTerminal(TransactionStatus.FAILED);
                log.error("Permanent failure: tx={} req={} reason={}",
                        r.transactionId(), r.requestId(), e.getMessage(), e);
                return;
            }
        }
    }

    private void handleInvalid(TransactionRequest r, String reason, String correlationId) {
        log.warn("Validation failed: req={} reason={}", correlationId, reason);
        report.onTerminal(TransactionStatus.FAILED);
        if (r == null || r.transactionId() == null || r.transactionId().isBlank()) {
            // Nothing durable to persist — in production this would go to a
            // DLQ with the raw payload (and a redaction policy) for triage.
            return;
        }
        TransactionRecord rec = new TransactionRecord(r);
        rec.setFailureReason(reason);
        rec.setStatus(TransactionStatus.FAILED);
        stateStore.putIfAbsent(rec);
    }

    private void markDuplicate(TransactionRequest r, TransactionRecord existing, String correlationId) {
        log.info("Duplicate transactionId: tx={} req={} existingStatus={} correlationId={}",
                r.transactionId(), r.requestId(), existing.status(), correlationId);
        report.onTerminal(TransactionStatus.DUPLICATE);
    }

    private static void sleepQuietly(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
