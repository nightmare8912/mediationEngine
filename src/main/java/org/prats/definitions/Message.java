
package org.prats.definitions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public final class Message {

    private final UUID messageId;
    private UUID correlationId;

    private MessageType messageType;
    private final ProcessType processType;

    private Operation operation;

    private final Instant timestamp;

    private JsonNode payload;

    @JsonIgnore
    private String fullStringOriginalMessage;

    private String recipientID;

    private String status;
    private Error error;


    // used when creating a new message with a random correlation ID.
    public Message(
            MessageType messageType,
            ProcessType processType,
            Operation operation,
            JsonNode payload,
            String fullStringOriginalMessage,
            String recipientID
    ) {
        this(
                UUID.randomUUID(),
                UUID.randomUUID(),
                messageType,
                processType,
                operation,
                payload,
                fullStringOriginalMessage,
                recipientID,
                Instant.now()
        );
    }


    // used when creating a message with an existing correlation ID.
    public Message(
            MessageType messageType,
            ProcessType processType,
            Operation operation,
            JsonNode payload,
            String fullStringOriginalMessage,
            String recipientID,
            UUID correlationId
    ) {
        this(
                UUID.randomUUID(),
                correlationId,
                messageType,
                processType,
                operation,
                payload,
                fullStringOriginalMessage,
                recipientID,
                Instant.now()
        );
    }


    // internal constructor shared by the public constructors and Jackson creator.
    private Message(
            UUID messageId,
            UUID correlationId,
            MessageType messageType,
            ProcessType processType,
            Operation operation,
            JsonNode payload,
            String fullStringOriginalMessage,
            String recipientID,
            Instant timestamp
    ) {
        this.messageId = Objects.requireNonNull(messageId, "messageId cannot be null");
        this.correlationId = Objects.requireNonNull(correlationId, "correlationId cannot be null");
        this.messageType = Objects.requireNonNull(messageType, "messageType cannot be null");
        this.processType = Objects.requireNonNull(processType, "processType cannot be null");
        this.operation = Objects.requireNonNull(operation, "operation cannot be null");
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp cannot be null");

        this.payload = payload;
        this.fullStringOriginalMessage = fullStringOriginalMessage;
        this.recipientID = recipientID;
    }


    // Jackson uses this constructor when deserializing JSON into a Message.
    // Both processType and messageSource are accepted to preserve the existing JSON representation.
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Message(
            @JsonProperty(value = "messageId", required = true)
            UUID messageId,

            @JsonProperty(value = "correlationId", required = true)
            UUID correlationId,

            @JsonProperty(value = "messageType", required = true)
            MessageType messageType,

            @JsonProperty("processType")
            ProcessType processType,

            @JsonProperty("messageSource")
            ProcessType messageSource,

            @JsonProperty(value = "operation", required = true)
            Operation operation,

            @JsonProperty(value = "timestamp", required = true)
            Instant timestamp
    ) {
        this(
                messageId,
                correlationId,
                messageType,
                processType != null ? processType : messageSource,
                operation,
                null,
                null,
                null,
                timestamp
        );
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

    public void generateRandomCorrelationId() {
        this.correlationId = UUID.randomUUID();
    }

    public MessageType getMessageType() {
        return messageType;
    }


    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    @JsonProperty("processType")
    public ProcessType getProcessType() {
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


    @JsonIgnore
    public String getFullStringOriginalMessage() {
        return fullStringOriginalMessage;
    }

    @JsonIgnore
    public void setFullStringOriginalMessage(String fullStringOriginalMessage) {
        this.fullStringOriginalMessage = fullStringOriginalMessage;
    }


    @JsonProperty("recipientID")
    public String getRecipientID() {
        return recipientID;
    }

    @JsonProperty("recipientID")
    public void setRecipientID(String recipientID) {
        this.recipientID = recipientID;
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
