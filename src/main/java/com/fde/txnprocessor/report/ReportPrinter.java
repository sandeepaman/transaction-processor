package com.fde.txnprocessor.report;

import com.fde.txnprocessor.model.TransactionRecord;
import com.fde.txnprocessor.model.TransactionStatus;
import com.fde.txnprocessor.store.InMemoryLedgerStore;
import com.fde.txnprocessor.store.TransactionStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Human-readable end-of-batch summary. Pulls counters from ProcessingReport
 * and final per-transaction state from TransactionStateStore so the operator
 * can reconcile the two views and spot drift (which would indicate a
 * bookkeeping bug).
 */
public final class ReportPrinter {

    private static final Logger log = LoggerFactory.getLogger(ReportPrinter.class);

    private ReportPrinter() {}

    public static void print(ProcessingReport report,
                             TransactionStateStore stateStore,
                             InMemoryLedgerStore ledger) {
        log.info("================= PROCESSING SUMMARY =================");
        log.info("Requests submitted : {}", report.requestsSubmitted());
        for (TransactionStatus s : TransactionStatus.values()) {
            log.info("  {} = {}", padRight(s.name(), 16), report.countOf(s));
        }
        log.info("Retry attempts     : {}", report.retriesTotal());

        log.info("------------------- TRANSACTION STATE ----------------");
        List<TransactionRecord> records = new ArrayList<>(stateStore.findAll());
        records.sort(Comparator.comparing(r -> r.request().transactionId()));
        for (TransactionRecord r : records) {
            log.info("  tx={} req={} account={} seq={} type={} status={} attempts={} reason={}",
                    r.request().transactionId(),
                    r.request().requestId(),
                    r.request().accountId(),
                    r.request().sequenceNumber(),
                    r.request().transactionType(),
                    r.status(),
                    r.attemptCount(),
                    r.failureReason() == null ? "" : r.failureReason());
        }

        log.info("---------------------- LEDGER ------------------------");
        Map<String, java.math.BigDecimal> balances = ledger.snapshotBalances();
        balances.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> log.info("  account={} balance={}", e.getKey(), e.getValue()));
        log.info("======================================================");
    }

    private static String padRight(String s, int width) {
        if (s.length() >= width) return s;
        return s + " ".repeat(width - s.length());
    }
}
