package com.fde.txnprocessor.ingestion;

import com.fde.txnprocessor.model.TransactionRequest;
import com.fde.txnprocessor.processor.TransactionProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives a batch end-to-end: pull every record from the source, submit each
 * to the processor, then drain any records parked by the ordering gate.
 *
 * Any exception thrown while handling a single record is caught and logged
 * so a single poison input never halts the batch (L2 acceptance criterion:
 * invalid records are isolated and do not stop the full batch).
 */
public class BackgroundIngestor {

    private static final Logger log = LoggerFactory.getLogger(BackgroundIngestor.class);

    private final TransactionSource source;
    private final TransactionProcessor processor;

    public BackgroundIngestor(TransactionSource source, TransactionProcessor processor) {
        this.source = source;
        this.processor = processor;
    }

    public void runBatch() {
        log.info("Starting batch ingestion");
        int submitted = 0;
        for (TransactionRequest r : source.read()) {
            try {
                processor.submit(r);
                submitted++;
            } catch (RuntimeException e) {
                // Belt-and-braces: processor.submit already handles its own
                // exceptions, but an unforeseen bug or OOM-style throw must
                // not take the loop down with it.
                log.error("Unhandled error submitting request req={}: {}",
                        r == null ? "null" : r.requestId(), e.getMessage(), e);
            }
        }
        log.info("Submitted {} record(s), draining pending-order buffer", submitted);
        processor.drainPending();
        log.info("Batch complete");
    }
}
