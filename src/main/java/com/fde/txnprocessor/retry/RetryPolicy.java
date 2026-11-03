package com.fde.txnprocessor.retry;

/**
 * Bounded retry policy. Decoupled from the clock so tests run in milliseconds.
 * Production would layer exponential backoff + jitter on top of this.
 */
public class RetryPolicy {

    private final int maxAttempts;
    private final long baseBackoffMillis;

    public RetryPolicy(int maxAttempts, long baseBackoffMillis) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (baseBackoffMillis < 0) {
            throw new IllegalArgumentException("baseBackoffMillis must be >= 0");
        }
        this.maxAttempts = maxAttempts;
        this.baseBackoffMillis = baseBackoffMillis;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    /** @param attemptCount number of attempts already made (1 after the first) */
    public boolean shouldRetry(int attemptCount) {
        return attemptCount < maxAttempts;
    }

    /** Backoff for the next attempt: linear in this impl, documented in DESIGN.md. */
    public long backoffMillis(int attemptCount) {
        return baseBackoffMillis * attemptCount;
    }
}
