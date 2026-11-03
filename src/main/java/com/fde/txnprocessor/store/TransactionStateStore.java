package com.fde.txnprocessor.store;

import com.fde.txnprocessor.model.TransactionRecord;

import java.util.Collection;
import java.util.Optional;

public interface TransactionStateStore {

    Optional<TransactionRecord> find(String transactionId);

    /**
     * Insert the record if no record exists for its transactionId.
     * Returns the already-present record if one exists, else the inserted one.
     * Enables callers to detect a replay of the same business transactionId.
     */
    TransactionRecord putIfAbsent(TransactionRecord record);

    Collection<TransactionRecord> findAll();
}
