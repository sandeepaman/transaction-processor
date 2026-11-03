package com.fde.txnprocessor.config;

import java.util.Set;

/**
 * Static runtime config. Centralised so a single place controls the knobs
 * tests and the CLI both lean on. In a Spring/Micronaut build this would be
 * backed by @ConfigurationProperties; here we keep the surface tiny.
 */
public record ProcessorConfig(
        long startingSequence,
        int maxAttempts,
        long baseBackoffMillis,
        Set<String> transientFailureTxnIds,
        String inputResource) {

    public static ProcessorConfig defaults() {
        return new ProcessorConfig(
                1L,
                3,
                10L,
                Set.of("TXN-2005"),
                "sample-transactions.json");
    }
}
