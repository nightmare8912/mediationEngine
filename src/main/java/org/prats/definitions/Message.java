package org.prats.definitions;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

public final class Message {

    private final UUID messageId;
    private UUID correlationId;

    private final MessageType messageType;
    private final ProcessType processType;

    private Operation operation;

    private final Instant timestamp;

    private JsonNode payload;

    private String status;
    private Error error;

    public Message(
            MessageType messageType,
            ProcessType processType,
            Operation operation,
            JsonNode payload
    ) {
        this.messageId = UUID.randomUUID();
        this.messageType = messageType;
        this.processType = processType;
        this.operation = operation;
        this.payload = payload;
        this.timestamp = Instant.now();
    }

    public Message(
            MessageType messageType,
            ProcessType processType,
            Operation operation,
            JsonNode payload,
            UUID correlationId
    ) {
        this.messageId = UUID.randomUUID();
        this.messageType = messageType;
        this.processType = processType;
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

    public MessageType getMessageType() {
        return messageType;
    }

    public ProcessType getMessageSource() {
        return processType;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
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

    public Error getError() {
        return error;
    }

    public void setError(Error error) {
        this.error = error;
    }
}