package com.fde.txnprocessor.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Immutable inbound transaction request. Treated as data from an untrusted upstream:
 * all validation happens in TransactionValidator before any processing.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TransactionRequest(
        String transactionId,
        String requestId,
        String accountId,
        TransactionType transactionType,
        BigDecimal amount,
        String currency,
        long sequenceNumber,
        Instant eventTime) {

    @JsonCreator
    public TransactionRequest(
            @JsonProperty("transactionId") String transactionId,
            @JsonProperty("requestId") String requestId,
            @JsonProperty("accountId") String accountId,
            @JsonProperty("transactionType") TransactionType transactionType,
            @JsonProperty("amount") BigDecimal amount,
            @JsonProperty("currency") String currency,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("eventTime") Instant eventTime) {
        this.transactionId = transactionId;
        this.requestId = requestId;
        this.accountId = accountId;
        this.transactionType = transactionType;
        this.amount = amount;
        this.currency = currency;
        this.sequenceNumber = sequenceNumber;
        this.eventTime = eventTime;
    }
}
