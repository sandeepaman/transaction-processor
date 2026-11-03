package com.fde.txnprocessor.validator;

public record ValidationResult(boolean valid, String reason) {

    private static final ValidationResult OK = new ValidationResult(true, null);

    public static ValidationResult ok() {
        return OK;
    }

    public static ValidationResult invalid(String reason) {
        return new ValidationResult(false, reason);
    }
}
