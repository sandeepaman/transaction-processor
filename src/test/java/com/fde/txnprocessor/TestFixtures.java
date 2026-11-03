package com.fde.txnprocessor;

import com.fde.txnprocessor.model.TransactionRequest;
import com.fde.txnprocessor.model.TransactionType;
import com.fde.txnprocessor.ordering.AccountSequenceTracker;
import com.fde.txnprocessor.processor.TransactionProcessor;
import com.fde.txnprocessor.report.ProcessingReport;
import com.fde.txnprocessor.retry.FailureSimulator;
import com.fde.txnprocessor.retry.RetryPolicy;
import com.fde.txnprocessor.store.InMemoryIdempotencyStore;
import com.fde.txnprocessor.store.InMemoryLedgerStore;
import com.fde.txnprocessor.store.InMemoryTransactionStateStore;
import com.fde.txnprocessor.validator.TransactionValidator;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;

/**
 * Test wiring helpers. Keeps individual tests focused on scenarios and
 * assertions, not boilerplate construction.
 */
public final class TestFixtures {

    private TestFixtures() {}

    public static TransactionRequest request(String txnId,
                                             String reqId,
                                             String accountId,
                                             TransactionType type,
                                             String amount,
                                             long seq) {
        return new TransactionRequest(
                txnId, reqId, accountId, type,
                new BigDecimal(amount), "USD", seq, Instant.now());
    }

    public static Wiring newWiring(long startingSequence,
                                   int maxAttempts,
                                   Set<String> transientFailOnce) {
        InMemoryIdempotencyStore idempotency = new InMemoryIdempotencyStore();
        InMemoryTransactionStateStore state = new InMemoryTransactionStateStore();
        InMemoryLedgerStore ledger = new InMemoryLedgerStore();
        AccountSequenceTracker tracker = new AccountSequenceTracker(startingSequence);
        RetryPolicy retryPolicy = new RetryPolicy(maxAttempts, 0L);
        FailureSimulator simulator = new FailureSimulator(transientFailOnce);
        TransactionValidator validator = new TransactionValidator();
        ProcessingReport report = new ProcessingReport();
        TransactionProcessor processor = new TransactionProcessor(
                validator, idempotency, state, ledger, tracker, retryPolicy, simulator, report);
        return new Wiring(idempotency, state, ledger, tracker, retryPolicy, simulator,
                validator, report, processor);
    }

    public static Wiring newWiring() {
        return newWiring(1L, 3, Set.of());
    }

    public record Wiring(
            InMemoryIdempotencyStore idempotency,
            InMemoryTransactionStateStore state,
            InMemoryLedgerStore ledger,
            AccountSequenceTracker tracker,
            RetryPolicy retryPolicy,
            FailureSimulator simulator,
            TransactionValidator validator,
            ProcessingReport report,
            TransactionProcessor processor) {}
}
