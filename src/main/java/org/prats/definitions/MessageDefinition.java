package org.prats.definitions;

import org.prats.messageHandler.MessageSource;
import org.prats.messageHandler.MessageType;

import java.util.UUID;

public final class MessageDefinition {
    private final MessageType messageType;
    private final UUID messageId;
    private UUID correlationId;
    private final MessageSource messageSource;
    private final String message;
    private String response;

    public MessageDefinition(
            MessageType     messageType,
            MessageSource   messageSource,
            String          message
    ) {
        messageId          = UUID.randomUUID();
        this.messageType   = messageType;
        this.messageSource = messageSource;
        this.message       = message;
    }

    public MessageDefinition(
            MessageType   messageType,
            MessageSource messageSource,
            String        message,
            UUID          correlationId
    ) {
        messageId          = UUID.randomUUID();
        this.messageType   = messageType;
        this.messageSource = messageSource;
        this.message       = message;
        this.correlationId = correlationId;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public MessageSource getMessageSource() {
        return messageSource;
    }

    public String getMessage() {
        return message;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public UUID getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(UUID correlationId) {
        this.correlationId = correlationId;
    }
}