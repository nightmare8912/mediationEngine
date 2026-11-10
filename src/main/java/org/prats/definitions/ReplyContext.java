package org.prats.definitions;

import java.util.Objects;
import java.util.UUID;

public record ReplyContext(
        UUID correlationId
) {
    public ReplyContext {
        Objects.requireNonNull(
                correlationId,
                "Correlation ID cannot be null"
        );
    }
}