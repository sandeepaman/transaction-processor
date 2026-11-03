package com.fde.txnprocessor.model;

import java.time.Instant;

/**
 * Mutable processing-state wrapper around an immutable TransactionRequest.
 * Owned by TransactionStateStore. All mutators are synchronized so retries
 * from the processor and reads from the reporter never see torn state.
 */
public class TransactionRecord {

    private final TransactionRequest request;
    private volatile TransactionStatus status;
    private volatile int attemptCount;
    private volatile String failureReason;
    private volatile Instant lastUpdatedAt;

    public TransactionRecord(TransactionRequest request) {
        this.request = request;
        this.status = TransactionStatus.RECEIVED;
        this.attemptCount = 0;
        this.lastUpdatedAt = Instant.now();
    }

    public TransactionRequest request() {
        return request;
    }

    public TransactionStatus status() {
        return status;
    }

    public int attemptCount() {
        return attemptCount;
    }

    public String failureReason() {
        return failureReason;
    }

    public Instant lastUpdatedAt() {
        return lastUpdatedAt;
    }

    public synchronized void setStatus(TransactionStatus next) {
        this.status = next;
        this.lastUpdatedAt = Instant.now();
    }

    public synchronized void incrementAttempt() {
        this.attemptCount++;
        this.lastUpdatedAt = Instant.now();
    }

    public synchronized void setFailureReason(String reason) {
        this.failureReason = reason;
        this.lastUpdatedAt = Instant.now();
    }
}
