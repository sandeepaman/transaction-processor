package com.fde.txnprocessor;

import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.model.TransactionType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static com.fde.txnprocessor.TestFixtures.request;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DuplicateDetectionTest {

    @Test
    void duplicateRequestId_isMarkedDuplicate_andLedgerIsNotTouchedTwice() {
        TestFixtures.Wiring w = TestFixtures.newWiring();

        w.processor().submit(request("TXN-1", "REQ-1", "ACC-1", TransactionType.CREDIT, "100", 1));
        // Same requestId arrives again — e.g. a client retried on a timeout
        // after the original succeeded.
        w.processor().submit(request("TXN-1", "REQ-1", "ACC-1", TransactionType.CREDIT, "100", 1));

        assertEquals(new BigDecimal("100"), w.ledger().balanceOf("ACC-1"));
        assertEquals(TransactionStatus.PROCESSED, w.state().find("TXN-1").orElseThrow().status());
        assertEquals(1, w.report().countOf(TransactionStatus.PROCESSED));
        assertEquals(1, w.report().countOf(TransactionStatus.DUPLICATE));
    }

    @Test
    void duplicateTransactionId_withDifferentRequestId_isMarkedDuplicate() {
        TestFixtures.Wiring w = TestFixtures.newWiring();

        w.processor().submit(request("TXN-1", "REQ-A", "ACC-1", TransactionType.CREDIT, "100", 1));
        // Same business txn, new idempotency key (e.g. a flawed client retry).
        // The transactionId-level dedupe should still catch it.
        w.processor().submit(request("TXN-1", "REQ-B", "ACC-1", TransactionType.CREDIT, "100", 1));

        assertEquals(new BigDecimal("100"), w.ledger().balanceOf("ACC-1"));
        assertEquals(1, w.report().countOf(TransactionStatus.PROCESSED));
        assertEquals(1, w.report().countOf(TransactionStatus.DUPLICATE));
    }

    @Test
    void duplicateAfterProcessing_doesNotMutateFinalState() {
        TestFixtures.Wiring w = TestFixtures.newWiring();

        w.processor().submit(request("TXN-1", "REQ-A", "ACC-1", TransactionType.CREDIT, "100", 1));
        w.processor().submit(request("TXN-1", "REQ-A", "ACC-1", TransactionType.CREDIT, "100", 1));
        w.processor().submit(request("TXN-1", "REQ-A", "ACC-1", TransactionType.CREDIT, "100", 1));

        assertEquals(new BigDecimal("100"), w.ledger().balanceOf("ACC-1"));
        assertTrue(w.ledger().hasBeenApplied("TXN-1"));
        assertEquals(2, w.report().countOf(TransactionStatus.DUPLICATE));
    }
}
