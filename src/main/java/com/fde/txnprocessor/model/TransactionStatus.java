package com.fde.txnprocessor.model;

/**
 * Lifecycle of a transaction request as it flows through the processor.
 * Transitions:
 *   RECEIVED -> PROCESSING -> PROCESSED | FAILED | RETRY_PENDING
 *   RECEIVED -> DUPLICATE  (short-circuit)
 *   RECEIVED -> PENDING_ORDER -> PROCESSING -> ...
 *   RETRY_PENDING -> PROCESSING -> PROCESSED | FAILED
 *   RECEIVED -> FAILED  (validation reject)
 */
public enum TransactionStatus {
    RECEIVED,
    PROCESSING,
    PROCESSED,
    DUPLICATE,
    FAILED,
    RETRY_PENDING,
    PENDING_ORDER
}
