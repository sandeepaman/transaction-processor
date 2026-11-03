package com.fde.txnprocessor.store;

import com.fde.txnprocessor.model.TransactionRequest;

import java.math.BigDecimal;

/**
 * Business side-effect sink. apply() is the only operation that moves money,
 * and it MUST be idempotent on transactionId: a second call for the same
 * transactionId is a no-op that returns false. This is the inner safety net
 * that preserves correctness even if upstream dedupe is bypassed (e.g. after
 * a crash-restart that re-plays the stream).
 */
public interface LedgerStore {

    /**
     * @return true if the ledger changed, false if the transactionId had
     *         already been applied (no-op). Never throws for a replay.
     */
    boolean apply(TransactionRequest request);

    BigDecimal balanceOf(String accountId);

    boolean hasBeenApplied(String transactionId);
}
