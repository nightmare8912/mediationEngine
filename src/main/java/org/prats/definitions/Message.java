package org.prats.definitions;

import com.fasterxml.jackson.annotation.JsonIgnore;
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

    private  JsonNode payload;

    @JsonIgnore
    private  String fullStringOriginalMessage; // jackson ignores this, the idea for this field is to retain the original message in the MessageClass but while parsing this should not be sent back to the caller, it's an internal tracker

    private String status;
    private Error error;

    public Message(
            MessageType messageType,
            ProcessType processType,
            Operation operation,
            JsonNode payload,
            String fullStringOriginalMessage
    ) {
        this.messageId                  = UUID.randomUUID();
        this.messageType                = messageType;
        this.processType                = processType;
        this.operation                  = operation;
        this.payload                    = payload;
        this.fullStringOriginalMessage  = fullStringOriginalMessage;
        this.timestamp = Instant.now();
    }

    public Message(
            MessageType messageType,
            ProcessType processType,
            Operation operation,
            JsonNode payload,
            String fullStringOriginalMessage,
            UUID correlationId
    ) {
        this.messageId                  = UUID.randomUUID();
        this.messageType                = messageType;
        this.processType                = processType;
        this.operation                  = operation;
        this.payload                    = payload;
        this.fullStringOriginalMessage  = fullStringOriginalMessage;
        this.correlationId              = correlationId;
        this.timestamp                  = Instant.now();
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

    public ProcessType getProcessType() {
        return processType;
    }

    public String getFullStringOriginalMessage() {
        return fullStringOriginalMessage;
    }

    public void setFullStringOriginalMessage(String fullStringOriginalMessage) {
        this.fullStringOriginalMessage = fullStringOriginalMessage;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Error getError() {
        return error;
    }

    public void setError(Error error) {
        this.error = error;
    }
}