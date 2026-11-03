package com.fde.txnprocessor;

import com.fde.txnprocessor.model.TransactionRequest;
import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.model.TransactionType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static com.fde.txnprocessor.TestFixtures.request;
import static org.junit.jupiter.api.Assertions.*;

class ValidationTest {

    @Test
    void negativeAmount_isMarkedFailed_andLedgerIsUntouched() {
        TestFixtures.Wiring w = TestFixtures.newWiring();

        w.processor().submit(request("TXN-BAD", "REQ-BAD", "ACC-1", TransactionType.DEBIT, "-10", 1));

        var rec = w.state().find("TXN-BAD").orElseThrow();
        assertEquals(TransactionStatus.FAILED, rec.status());
        assertTrue(rec.failureReason().contains("positive"));
        assertEquals(BigDecimal.ZERO, w.ledger().balanceOf("ACC-1"));
    }

    @Test
    void malformedRecord_isIsolated_andNextRecordStillProcesses() {
        TestFixtures.Wiring w = TestFixtures.newWiring();

        TransactionRequest bad = new TransactionRequest(
                "TXN-NULLACC", "REQ-X", null,
                TransactionType.CREDIT, new BigDecimal("10"), "USD", 1, Instant.now());

        w.processor().submit(bad);
        w.processor().submit(request("TXN-OK", "REQ-OK", "ACC-1", TransactionType.CREDIT, "25", 1));

        var badRec = w.state().find("TXN-NULLACC").orElseThrow();
        assertEquals(TransactionStatus.FAILED, badRec.status());
        assertEquals(TransactionStatus.PROCESSED, w.state().find("TXN-OK").orElseThrow().status());
        assertEquals(new BigDecimal("25"), w.ledger().balanceOf("ACC-1"));
    }

    @Test
    void unsupportedCurrency_isRejected() {
        TestFixtures.Wiring w = TestFixtures.newWiring();

        TransactionRequest r = new TransactionRequest(
                "TXN-XYZ", "REQ-XYZ", "ACC-1",
                TransactionType.CREDIT, new BigDecimal("10"), "XYZ", 1, Instant.now());
        w.processor().submit(r);

        var rec = w.state().find("TXN-XYZ").orElseThrow();
        assertEquals(TransactionStatus.FAILED, rec.status());
        assertTrue(rec.failureReason().toLowerCase().contains("currency"));
    }
}
