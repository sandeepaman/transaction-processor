package com.fde.txnprocessor.validator;

import com.fde.txnprocessor.model.TransactionRequest;

import java.math.BigDecimal;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Boundary validator. Rejects malformed input before any state is touched,
 * so a single poison record never halts the batch and never leaks into the
 * ledger. Reasons are human-readable but never echo raw field values that
 * could carry PII; see security notes in DESIGN.md.
 */
public class TransactionValidator {

    private static final Pattern CURRENCY = Pattern.compile("^[A-Z]{3}$");
    private static final Set<String> SUPPORTED_CURRENCIES = Set.of(
            "USD", "EUR", "GBP", "INR", "JPY", "AUD", "CAD", "SGD");

    public ValidationResult validate(TransactionRequest r) {
        if (r == null) {
            return ValidationResult.invalid("null request");
        }
        if (isBlank(r.transactionId())) {
            return ValidationResult.invalid("missing transactionId");
        }
        if (isBlank(r.requestId())) {
            return ValidationResult.invalid("missing requestId");
        }
        if (isBlank(r.accountId())) {
            return ValidationResult.invalid("missing accountId");
        }
        if (r.transactionType() == null) {
            return ValidationResult.invalid("missing transactionType");
        }
        if (r.amount() == null) {
            return ValidationResult.invalid("missing amount");
        }
        if (r.amount().signum() <= 0) {
            return ValidationResult.invalid("amount must be positive");
        }
        if (r.amount().compareTo(new BigDecimal("1000000000")) > 0) {
            return ValidationResult.invalid("amount exceeds max permitted");
        }
        if (isBlank(r.currency())) {
            return ValidationResult.invalid("missing currency");
        }
        if (!CURRENCY.matcher(r.currency()).matches()) {
            return ValidationResult.invalid("currency must be 3-letter ISO code");
        }
        if (!SUPPORTED_CURRENCIES.contains(r.currency())) {
            return ValidationResult.invalid("unsupported currency");
        }
        if (r.sequenceNumber() < 0) {
            return ValidationResult.invalid("sequenceNumber must be non-negative");
        }
        return ValidationResult.ok();
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
