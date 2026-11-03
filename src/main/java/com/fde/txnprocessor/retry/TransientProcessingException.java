package com.fde.txnprocessor.retry;

/**
 * Marker for failures the processor should retry (network blip, downstream
 * throttle, lock contention). Anything else is terminal.
 */
public class TransientProcessingException extends RuntimeException {

    public TransientProcessingException(String message) {
        super(message);
    }

    public TransientProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
