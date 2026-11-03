package com.fde.txnprocessor;

import com.fde.txnprocessor.config.ProcessorConfig;
import com.fde.txnprocessor.ingestion.BackgroundIngestor;
import com.fde.txnprocessor.ingestion.JsonFileTransactionSource;
import com.fde.txnprocessor.ingestion.TransactionSource;
import com.fde.txnprocessor.ordering.AccountSequenceTracker;
import com.fde.txnprocessor.processor.TransactionProcessor;
import com.fde.txnprocessor.report.ProcessingReport;
import com.fde.txnprocessor.report.ReportPrinter;
import com.fde.txnprocessor.retry.FailureSimulator;
import com.fde.txnprocessor.retry.RetryPolicy;
import com.fde.txnprocessor.store.IdempotencyStore;
import com.fde.txnprocessor.store.InMemoryIdempotencyStore;
import com.fde.txnprocessor.store.InMemoryLedgerStore;
import com.fde.txnprocessor.store.InMemoryTransactionStateStore;
import com.fde.txnprocessor.store.TransactionStateStore;
import com.fde.txnprocessor.validator.TransactionValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CLI entry point. Wires the components and runs one batch.
 *
 * Usage:
 *   java -jar transaction-processor.jar [input-location]
 *
 * input-location is a classpath resource (default: sample-transactions.json)
 * or a filesystem path.
 */
public final class TransactionProcessorApplication {

    private static final Logger log = LoggerFactory.getLogger(TransactionProcessorApplication.class);

    private TransactionProcessorApplication() {}

    public static void main(String[] args) {
        ProcessorConfig config = ProcessorConfig.defaults();
        String inputLocation = args.length > 0 ? args[0] : config.inputResource();
        log.info("Transaction Processor starting (input='{}', maxAttempts={}, startingSeq={})",
                inputLocation, config.maxAttempts(), config.startingSequence());

        IdempotencyStore idempotencyStore = new InMemoryIdempotencyStore();
        TransactionStateStore stateStore = new InMemoryTransactionStateStore();
        InMemoryLedgerStore ledgerStore = new InMemoryLedgerStore();
        AccountSequenceTracker tracker = new AccountSequenceTracker(config.startingSequence());
        RetryPolicy retryPolicy = new RetryPolicy(config.maxAttempts(), config.baseBackoffMillis());
        FailureSimulator simulator = new FailureSimulator(config.transientFailureTxnIds());
        TransactionValidator validator = new TransactionValidator();
        ProcessingReport report = new ProcessingReport();

        TransactionProcessor processor = new TransactionProcessor(
                validator, idempotencyStore, stateStore, ledgerStore,
                tracker, retryPolicy, simulator, report);

        TransactionSource source = new JsonFileTransactionSource(inputLocation);
        BackgroundIngestor ingestor = new BackgroundIngestor(source, processor);

        ingestor.runBatch();
        ReportPrinter.print(report, stateStore, ledgerStore);
    }
}
