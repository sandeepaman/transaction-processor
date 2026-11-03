package com.fde.txnprocessor.store;

import com.fde.txnprocessor.model.TransactionRecord;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryTransactionStateStore implements TransactionStateStore {

    private final ConcurrentHashMap<String, TransactionRecord> byTransactionId = new ConcurrentHashMap<>();

    @Override
    public Optional<TransactionRecord> find(String transactionId) {
        return Optional.ofNullable(byTransactionId.get(transactionId));
    }

    @Override
    public TransactionRecord putIfAbsent(TransactionRecord record) {
        TransactionRecord existing = byTransactionId.putIfAbsent(
                record.request().transactionId(), record);
        return existing != null ? existing : record;
    }

    @Override
    public Collection<TransactionRecord> findAll() {
        return byTransactionId.values();
    }
}
