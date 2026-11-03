package com.fde.txnprocessor.ordering;

import com.fde.txnprocessor.model.TransactionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-account strict-forward sequence tracker (Option A in the brief).
 *
 * Each account has an expected next sequence (starts at startingSequence).
 * Arrivals matching expected are released immediately; higher sequences are
 * parked in a per-account TreeMap keyed by sequence; lower sequences are
 * returned to the caller (duplicate/late — upstream dedupe handles it).
 *
 * drainAll() releases any still-parked records at end-of-batch. This covers
 * accounts whose very first observed event has sequence > startingSequence
 * (e.g. an account that only appears in this batch at seq=5), while keeping
 * the trade-off visible rather than silently relaxed. In production the same
 * mechanism would run on a time-based grace window and route still-missing
 * predecessors to a dead-letter queue — see DESIGN.md.
 */
public class AccountSequenceTracker {

    private static final Logger log = LoggerFactory.getLogger(AccountSequenceTracker.class);

    private final long startingSequence;
    private final Map<String, Long> nextExpected = new ConcurrentHashMap<>();
    private final Map<String, TreeMap<Long, TransactionRecord>> pending = new ConcurrentHashMap<>();

    public AccountSequenceTracker(long startingSequence) {
        this.startingSequence = startingSequence;
    }

    /**
     * Offer a record to the tracker.
     * @return records that are ready to process now, in sequence order.
     *         Empty if the record was parked.
     */
    public synchronized List<TransactionRecord> accept(TransactionRecord record) {
        String accountId = record.request().accountId();
        long seq = record.request().sequenceNumber();
        long expected = nextExpected.computeIfAbsent(accountId, k -> startingSequence);

        if (seq == expected) {
            List<TransactionRecord> ready = new ArrayList<>();
            ready.add(record);
            long nextSeq = expected + 1;
            TreeMap<Long, TransactionRecord> parked = pending.get(accountId);
            while (parked != null && parked.containsKey(nextSeq)) {
                ready.add(parked.remove(nextSeq));
                nextSeq++;
            }
            nextExpected.put(accountId, nextSeq);
            log.debug("Released {} record(s) for account={} starting at seq={}",
                    ready.size(), accountId, seq);
            return ready;
        }

        if (seq > expected) {
            pending.computeIfAbsent(accountId, k -> new TreeMap<>()).put(seq, record);
            log.info("Parked (pending-order): tx={} account={} seq={} expected={}",
                    record.request().transactionId(), accountId, seq, expected);
            return List.of();
        }

        // seq < expected: this slot already passed. Could be a duplicate replay,
        // or a late-arriving event we've logically skipped. Hand it back so the
        // processor can run its dedupe/validation path and emit the correct status.
        log.info("Late arrival: tx={} account={} seq={} expected={}",
                record.request().transactionId(), accountId, seq, expected);
        return List.of(record);
    }

    /**
     * Flush every parked record across all accounts, lowest seq first per account.
     * Called at end-of-batch so an account whose first observed event is at a
     * sequence greater than startingSequence still gets processed.
     */
    public synchronized List<TransactionRecord> drainAll() {
        List<TransactionRecord> drained = new ArrayList<>();
        for (Map.Entry<String, TreeMap<Long, TransactionRecord>> e : pending.entrySet()) {
            Collection<TransactionRecord> values = e.getValue().values();
            drained.addAll(values);
            // Advance expected past the drained range so a subsequent in-order
            // arrival after drain is still handled correctly.
            if (!values.isEmpty()) {
                long maxSeq = e.getValue().lastKey();
                nextExpected.put(e.getKey(), maxSeq + 1);
            }
        }
        pending.clear();
        return drained;
    }

    public synchronized int pendingCount() {
        return pending.values().stream().mapToInt(TreeMap::size).sum();
    }
}
