package com.fde.txnprocessor.store;

/**
 * Deduplication store keyed by requestId (the upstream idempotency key).
 * markSeen must be atomic: it returns true only to the single caller that
 * won the race, so no two threads can both proceed with the same requestId.
 *
 * The interface is deliberately storage-agnostic — a production impl would
 * typically be a database row with a UNIQUE constraint, or a Redis SETNX.
 */
public interface IdempotencyStore {

    /**
     * @return true if this call registered the key (first-time), false if
     *         the key had already been registered by someone else.
     */
    boolean markSeen(String key);

    boolean hasSeen(String key);
}
