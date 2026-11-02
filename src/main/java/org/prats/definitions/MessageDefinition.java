package org.prats.definitions;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

public final class MessageDefinition {

    private final UUID messageId;
    private UUID correlationId;

    private final MessageTypeDefinition messageTypeDefinition;
    private final ProcessTypeDefinition processTypeDefinition;

    private OperationDefinition operation;

    private final Instant timestamp;

    private JsonNode payload;

    private String status;
    private ErrorDefinition error;

    public MessageDefinition(
            MessageTypeDefinition messageTypeDefinition,
            ProcessTypeDefinition processTypeDefinition,
            OperationDefinition operation,
            JsonNode payload
    ) {
        this.messageId = UUID.randomUUID();
        this.messageTypeDefinition = messageTypeDefinition;
        this.processTypeDefinition = processTypeDefinition;
        this.operation = operation;
        this.payload = payload;
        this.timestamp = Instant.now();
    }

    public MessageDefinition(
            MessageTypeDefinition messageTypeDefinition,
            ProcessTypeDefinition processTypeDefinition,
            OperationDefinition operation,
            JsonNode payload,
            UUID correlationId
    ) {
        this.messageId = UUID.randomUUID();
        this.messageTypeDefinition = messageTypeDefinition;
        this.processTypeDefinition = processTypeDefinition;
        this.operation = operation;
        this.payload = payload;
        this.correlationId = correlationId;
        this.timestamp = Instant.now();
    }

    public UUID getMessageId() {
        return messageId;
    }

    public UUID getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(UUID correlationId) {
        this.correlationId = correlationId;
    }

    public MessageTypeDefinition getMessageType() {
        return messageTypeDefinition;
    }

    public ProcessTypeDefinition getMessageSource() {
        return processTypeDefinition;
    }

    public OperationDefinition getOperation() {
        return operation;
    }

    public void setOperation(OperationDefinition operation) {
        this.operation = operation;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public JsonNode getPayload() {
        return payload;
    }

    public void setPayload(JsonNode payload) {
        this.payload = payload;
    }

    public ErrorDefinition getError() {
        return error;
    }

    public void setError(ErrorDefinition error) {
        this.error = error;
    }
}