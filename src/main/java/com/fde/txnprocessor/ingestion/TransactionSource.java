package com.fde.txnprocessor.ingestion;

import com.fde.txnprocessor.model.TransactionRequest;

/**
 * Pluggable input. In this build we read from a JSON file; the same contract
 * could sit behind a JMS/Kafka consumer, an HTTP endpoint, a database poller,
 * or an in-process queue — the processor doesn't care which.
 */
public interface TransactionSource {

    Iterable<TransactionRequest> read();
}
