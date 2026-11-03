package com.fde.txnprocessor;

import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.model.TransactionType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static com.fde.txnprocessor.TestFixtures.request;
import static org.junit.jupiter.api.Assertions.*;

class OutOfOrderTest {

    @Test
    void futureSequence_isParked_untilPredecessorArrives() {
        TestFixtures.Wiring w = TestFixtures.newWiring();

        // seq=2 arrives first -> parked (we expect seq=1 first)
        w.processor().submit(request("TXN-2", "REQ-2", "ACC-1", TransactionType.DEBIT, "40", 2));
        assertEquals(TransactionStatus.PENDING_ORDER, w.state().find("TXN-2").orElseThrow().status());
        assertEquals(BigDecimal.ZERO, w.ledger().balanceOf("ACC-1"));
        assertEquals(1, w.tracker().pendingCount());

        // seq=1 arrives -> processes seq=1 AND drains seq=2 in order
        w.processor().submit(request("TXN-1", "REQ-1", "ACC-1", TransactionType.CREDIT, "100", 1));

        assertEquals(TransactionStatus.PROCESSED, w.state().find("TXN-1").orElseThrow().status());
        assertEquals(TransactionStatus.PROCESSED, w.state().find("TXN-2").orElseThrow().status());
        assertEquals(new BigDecimal("60"), w.ledger().balanceOf("ACC-1"));
        assertEquals(0, w.tracker().pendingCount());
    }

    @Test
    void differentAccounts_doNotBlockEachOther() {
        TestFixtures.Wiring w = TestFixtures.newWiring();

        // ACC-1 waits for seq=1; ACC-2 waits for seq=1 independently.
        w.processor().submit(request("TXN-A1", "REQ-A1", "ACC-1", TransactionType.CREDIT, "50", 1));
        w.processor().submit(request("TXN-B2", "REQ-B2", "ACC-2", TransactionType.CREDIT, "70", 2));

        assertEquals(TransactionStatus.PROCESSED, w.state().find("TXN-A1").orElseThrow().status());
        assertEquals(TransactionStatus.PENDING_ORDER, w.state().find("TXN-B2").orElseThrow().status());
        assertEquals(new BigDecimal("50"), w.ledger().balanceOf("ACC-1"));
        assertEquals(BigDecimal.ZERO, w.ledger().balanceOf("ACC-2"));
    }

    @Test
    void drainPending_releasesRecordsWhoseFirstSeqExceedsStartingSequence() {
        TestFixtures.Wiring w = TestFixtures.newWiring();

        // First-ever observation for ACC-9 is seq=5. It stays parked...
        w.processor().submit(request("TXN-9", "REQ-9", "ACC-9", TransactionType.CREDIT, "500", 5));
        assertEquals(TransactionStatus.PENDING_ORDER, w.state().find("TXN-9").orElseThrow().status());

        // ...until the batch-end drain, which releases it.
        w.processor().drainPending();

        assertEquals(TransactionStatus.PROCESSED, w.state().find("TXN-9").orElseThrow().status());
        assertEquals(new BigDecimal("500"), w.ledger().balanceOf("ACC-9"));
    }

    @Test
    void drainedBatch_producesCorrectCountsBySource() {
        // Mirrors the L2 brief test data table in file order.
        TestFixtures.Wiring w = TestFixtures.newWiring();

        w.processor().submit(request("TXN-2002", "REQ-2",     "ACC-201", TransactionType.DEBIT,  "40",  2));
        w.processor().submit(request("TXN-2001", "REQ-1",     "ACC-201", TransactionType.CREDIT, "100", 1));
        w.processor().submit(request("TXN-2002", "REQ-2-DUP", "ACC-201", TransactionType.DEBIT,  "40",  2));
        w.processor().submit(request("TXN-2003", "REQ-3",     "ACC-201", TransactionType.CREDIT, "25",  3));
        w.processor().submit(request("TXN-2005", "REQ-5",     "ACC-203", TransactionType.CREDIT, "500", 5));
        w.processor().drainPending();

        assertAll(
                () -> assertEquals(TransactionStatus.PROCESSED,
                        w.state().find("TXN-2001").orElseThrow().status()),
                () -> assertEquals(TransactionStatus.PROCESSED,
                        w.state().find("TXN-2002").orElseThrow().status()),
                () -> assertEquals(TransactionStatus.PROCESSED,
                        w.state().find("TXN-2003").orElseThrow().status()),
                () -> assertEquals(TransactionStatus.PROCESSED,
                        w.state().find("TXN-2005").orElseThrow().status()),
                () -> assertEquals(new BigDecimal("85"), w.ledger().balanceOf("ACC-201")),
                () -> assertEquals(new BigDecimal("500"), w.ledger().balanceOf("ACC-203")),
                () -> assertEquals(1, w.report().countOf(TransactionStatus.DUPLICATE)),
                () -> assertEquals(4, w.report().countOf(TransactionStatus.PROCESSED))
        );
    }
}
