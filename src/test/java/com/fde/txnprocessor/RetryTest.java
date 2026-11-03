package com.fde.txnprocessor;

import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.model.TransactionType;
import com.fde.txnprocessor.ordering.AccountSequenceTracker;
import com.fde.txnprocessor.processor.TransactionProcessor;
import com.fde.txnprocessor.report.ProcessingReport;
import com.fde.txnprocessor.retry.FailureSimulator;
import com.fde.txnprocessor.retry.RetryPolicy;
import com.fde.txnprocessor.retry.TransientProcessingException;
import com.fde.txnprocessor.store.InMemoryIdempotencyStore;
import com.fde.txnprocessor.store.InMemoryLedgerStore;
import com.fde.txnprocessor.store.InMemoryTransactionStateStore;
import com.fde.txnprocessor.store.LedgerStore;
import com.fde.txnprocessor.validator.TransactionValidator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Set;

import static com.fde.txnprocessor.TestFixtures.request;
import static org.junit.jupiter.api.Assertions.*;

class RetryTest {

    @Test
    void transientFailureOnFirstAttempt_isRetriedAndSucceeds_withSingleLedgerEntry() {
        TestFixtures.Wiring w = TestFixtures.newWiring(1L, 3, Set.of("TXN-FLAKY"));

        w.processor().submit(request("TXN-FLAKY", "REQ-FLAKY", "ACC-1", TransactionType.CREDIT, "500", 1));

        var rec = w.state().find("TXN-FLAKY").orElseThrow();
        assertEquals(TransactionStatus.PROCESSED, rec.status());
        assertEquals(2, rec.attemptCount());
        assertEquals(new BigDecimal("500"), w.ledger().balanceOf("ACC-1"));
        assertEquals(1, w.report().countOf(TransactionStatus.PROCESSED));
        assertEquals(1, w.report().retriesTotal());
    }

    @Test
    void permanentlyFailingLedger_exhaustsRetries_andIsMarkedFailed() {
        // Hand-wire a ledger that always throws a transient so we can exercise
        // retry exhaustion without pulling in the simulator semantics.
        TransactionValidator validator = new TransactionValidator();
        InMemoryIdempotencyStore idempotency = new InMemoryIdempotencyStore();
        InMemoryTransactionStateStore state = new InMemoryTransactionStateStore();
        LedgerStore alwaysFailing = new LedgerStore() {
            @Override public boolean apply(com.fde.txnprocessor.model.TransactionRequest r) {
                throw new TransientProcessingException("downstream unavailable");
            }
            @Override public BigDecimal balanceOf(String a) { return BigDecimal.ZERO; }
            @Override public boolean hasBeenApplied(String t) { return false; }
        };
        AccountSequenceTracker tracker = new AccountSequenceTracker(1L);
        RetryPolicy policy = new RetryPolicy(3, 0L);
        FailureSimulator simulator = FailureSimulator.disabled();
        ProcessingReport report = new ProcessingReport();
        TransactionProcessor processor = new TransactionProcessor(
                validator, idempotency, state, alwaysFailing, tracker, policy, simulator, report);

        processor.submit(request("TXN-DOOMED", "REQ-D", "ACC-X", TransactionType.CREDIT, "10", 1));

        var rec = state.find("TXN-DOOMED").orElseThrow();
        assertEquals(TransactionStatus.FAILED, rec.status());
        assertEquals(3, rec.attemptCount());
        assertTrue(rec.failureReason().contains("downstream"));
        assertEquals(3, report.retriesTotal());
        assertEquals(1, report.countOf(TransactionStatus.FAILED));
    }

    @Test
    void innerLedgerIdempotency_preventsDoubleApplyAcrossRetries() {
        // Hand-wire a ledger that passes the first apply() through to a real
        // in-memory ledger, but throws a transient on the first call so the
        // processor retries. The real ledger's internal guard must still
        // ensure the retry does not double-apply.
        InMemoryLedgerStore underlying = new InMemoryLedgerStore();
        TransactionValidator validator = new TransactionValidator();
        InMemoryIdempotencyStore idempotency = new InMemoryIdempotencyStore();
        InMemoryTransactionStateStore state = new InMemoryTransactionStateStore();
        AccountSequenceTracker tracker = new AccountSequenceTracker(1L);
        RetryPolicy policy = new RetryPolicy(3, 0L);
        FailureSimulator simulator = FailureSimulator.disabled();
        ProcessingReport report = new ProcessingReport();

        // Compose a ledger that applies, then throws transient AFTER success —
        // simulating a crash between ledger write and ack.
        LedgerStore applyThenCrashOnce = new LedgerStore() {
            private int calls = 0;
            @Override public boolean apply(com.fde.txnprocessor.model.TransactionRequest r) {
                calls++;
                boolean applied = underlying.apply(r);
                if (calls == 1) {
                    throw new TransientProcessingException("ack lost after apply");
                }
                return applied;
            }
            @Override public BigDecimal balanceOf(String a) { return underlying.balanceOf(a); }
            @Override public boolean hasBeenApplied(String t) { return underlying.hasBeenApplied(t); }
        };

        TransactionProcessor processor = new TransactionProcessor(
                validator, idempotency, state, applyThenCrashOnce,
                tracker, policy, simulator, report);

        processor.submit(request("TXN-ACKLOST", "REQ-A", "ACC-Y", TransactionType.CREDIT, "100", 1));

        // Only ONE balance change, even though the processor retried.
        assertEquals(new BigDecimal("100"), underlying.balanceOf("ACC-Y"));
        assertEquals(TransactionStatus.PROCESSED,
                state.find("TXN-ACKLOST").orElseThrow().status());
    }
}
