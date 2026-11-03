package com.fde.txnprocessor.report;

import com.fde.txnprocessor.model.TransactionStatus;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mutable, thread-safe counters used for the end-of-batch summary.
 *
 * We count per-request events (not per-transactionId), so a duplicate replay
 * of a request shows up as a separate DUPLICATE count even though the state
 * store only ever holds one record per transactionId.
 */
public class ProcessingReport {

    private final Map<TransactionStatus, AtomicInteger> counts = new EnumMap<>(TransactionStatus.class);
    private final AtomicLong retriesTotal = new AtomicLong();
    private final AtomicLong requestsSubmitted = new AtomicLong();

    public ProcessingReport() {
        for (TransactionStatus s : TransactionStatus.values()) {
            counts.put(s, new AtomicInteger());
        }
    }

    public void onSubmitted() {
        requestsSubmitted.incrementAndGet();
    }

    public void onTerminal(TransactionStatus status) {
        counts.get(status).incrementAndGet();
    }

    public void onRetryAttempt() {
        retriesTotal.incrementAndGet();
    }

    public int countOf(TransactionStatus status) {
        return counts.get(status).get();
    }

    public long retriesTotal() {
        return retriesTotal.get();
    }

    public long requestsSubmitted() {
        return requestsSubmitted.get();
    }

    public Map<TransactionStatus, Integer> snapshot() {
        Map<TransactionStatus, Integer> out = new EnumMap<>(TransactionStatus.class);
        counts.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }
}
