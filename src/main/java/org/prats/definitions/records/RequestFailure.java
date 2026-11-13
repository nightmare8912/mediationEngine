package org.prats.definitions.records;

import java.util.Objects;

public record RequestFailure(
        String errorCode,
        String errorMessage,
        Throwable cause
) {

    public RequestFailure {

        Objects.requireNonNull(errorCode, "Error code cannot be null");
        Objects.requireNonNull(errorMessage, "Error message cannot be null");

        if (errorCode.isBlank()) {
            throw new IllegalArgumentException(
                    "Error code cannot be blank"
            );
        }
    }

    // Constructor for failures without an underlying exception
    public RequestFailure(
            String errorCode,
            String errorMessage
    ) {
        this(errorCode, errorMessage, null);
    }
}