package com.fde.txnprocessor.store;

import java.util.concurrent.ConcurrentHashMap;

public class InMemoryIdempotencyStore implements IdempotencyStore {

    private final ConcurrentHashMap<String, Boolean> seen = new ConcurrentHashMap<>();

    @Override
    public boolean markSeen(String key) {
        return seen.putIfAbsent(key, Boolean.TRUE) == null;
    }

    @Override
    public boolean hasSeen(String key) {
        return seen.containsKey(key);
    }
}
