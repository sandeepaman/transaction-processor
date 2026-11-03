package com.fde.txnprocessor.retry;

import com.fde.txnprocessor.model.TransactionRequest;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Deterministic transient-failure injector used for the assessment's retry
 * scenario (L2-FR08). Any transactionId in failOnceIds fails on its first
 * processing attempt with a TransientProcessingException, then succeeds on
 * the retry. This lets tests exercise the retry path without a real
 * downstream to flake.
 *
 * In production we would NOT inject failures; this exists purely so the
 * retry pipeline can be demonstrated end-to-end.
 */
public class FailureSimulator {

    private final Set<String> failOnceIds;
    private final ConcurrentHashMap<String, Integer> attemptsSeen = new ConcurrentHashMap<>();

    public FailureSimulator(Set<String> failOnceIds) {
        this.failOnceIds = Set.copyOf(failOnceIds);
    }

    public static FailureSimulator disabled() {
        return new FailureSimulator(Set.of());
    }

    public void maybeFail(TransactionRequest r) {
        if (!failOnceIds.contains(r.transactionId())) {
            return;
        }
        int seen = attemptsSeen.merge(r.transactionId(), 1, Integer::sum);
        if (seen == 1) {
            throw new TransientProcessingException(
                    "simulated transient failure on first attempt for " + r.transactionId());
        }
    }
}
