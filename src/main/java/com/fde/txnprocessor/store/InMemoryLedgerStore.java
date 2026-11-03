package com.fde.txnprocessor.store;

import com.fde.txnprocessor.model.TransactionRequest;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryLedgerStore implements LedgerStore {

    private final ConcurrentHashMap<String, BigDecimal> balances = new ConcurrentHashMap<>();
    private final Set<String> appliedTransactionIds = ConcurrentHashMap.newKeySet();
    private final Object lock = new Object();

    @Override
    public boolean apply(TransactionRequest r) {
        // Inner idempotency guard. We take a short monitor around the two-step
        // "check + add + mutate balance" so a concurrent duplicate of the same
        // transactionId cannot slip past the dedupe check and double-credit.
        synchronized (lock) {
            if (!appliedTransactionIds.add(r.transactionId())) {
                return false;
            }
            BigDecimal current = balances.getOrDefault(r.accountId(), BigDecimal.ZERO);
            BigDecimal next = switch (r.transactionType()) {
                case CREDIT -> current.add(r.amount());
                case DEBIT -> current.subtract(r.amount());
                case REVERSAL -> current.subtract(r.amount());
            };
            balances.put(r.accountId(), next);
            return true;
        }
    }

    @Override
    public BigDecimal balanceOf(String accountId) {
        return balances.getOrDefault(accountId, BigDecimal.ZERO);
    }

    @Override
    public boolean hasBeenApplied(String transactionId) {
        return appliedTransactionIds.contains(transactionId);
    }

    public Map<String, BigDecimal> snapshotBalances() {
        return Collections.unmodifiableMap(new ConcurrentHashMap<>(balances));
    }
}
