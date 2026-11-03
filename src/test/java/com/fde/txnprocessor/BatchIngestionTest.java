package com.fde.txnprocessor;

import com.fde.txnprocessor.ingestion.BackgroundIngestor;
import com.fde.txnprocessor.ingestion.JsonFileTransactionSource;
import com.fde.txnprocessor.model.TransactionStatus;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end sanity test over the bundled sample-transactions.json, which is
 * the exact table shipped in the L2 brief. Covers:
 *   - out-of-order within an account (TXN-2002 arrives before TXN-2001)
 *   - duplicate replay (REQ-2-DUP)
 *   - validation reject (TXN-2004 has negative amount)
 *   - retry-then-success (TXN-2005 configured as fail-once)
 *   - drain-pending for an account whose only event is seq > starting
 */
class BatchIngestionTest {

    @Test
    void runningTheBundledSampleProducesExpectedTerminalState() {
        TestFixtures.Wiring w = TestFixtures.newWiring(1L, 3, java.util.Set.of("TXN-2005"));
        BackgroundIngestor ingestor = new BackgroundIngestor(
                new JsonFileTransactionSource("sample-transactions.json"), w.processor());

        ingestor.runBatch();

        assertEquals(TransactionStatus.PROCESSED, w.state().find("TXN-2001").orElseThrow().status());
        assertEquals(TransactionStatus.PROCESSED, w.state().find("TXN-2002").orElseThrow().status());
        assertEquals(TransactionStatus.PROCESSED, w.state().find("TXN-2003").orElseThrow().status());
        assertEquals(TransactionStatus.FAILED,    w.state().find("TXN-2004").orElseThrow().status());
        assertEquals(TransactionStatus.PROCESSED, w.state().find("TXN-2005").orElseThrow().status());

        assertEquals(new BigDecimal("85"),  w.ledger().balanceOf("ACC-201"));
        assertEquals(BigDecimal.ZERO,       w.ledger().balanceOf("ACC-202"));
        assertEquals(new BigDecimal("500"), w.ledger().balanceOf("ACC-203"));

        assertEquals(4, w.report().countOf(TransactionStatus.PROCESSED));
        assertEquals(1, w.report().countOf(TransactionStatus.FAILED));
        assertEquals(1, w.report().countOf(TransactionStatus.DUPLICATE));
        assertEquals(1, w.report().retriesTotal(), "TXN-2005 retries once before success");
    }
}
