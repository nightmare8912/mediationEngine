
package org.prats.parsers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.prats.definitions.classes.Error;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.enums.ProcessType;
import org.prats.loggers.FileLogger;
import org.prats.loggers.EngineLogger;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class MessageParser {

    private static final EngineLogger log =
            EngineLogger.getLogger(MessageParser.class);

    private final ObjectMapper objectMapper;
    private final ProcessType processType;




    public MessageParser(ProcessType processType) {

        this.processType = processType;

        this.objectMapper = new ObjectMapper();

        objectMapper.registerModule(new JavaTimeModule());

        objectMapper.disable(
                SerializationFeature.WRITE_DATES_AS_TIMESTAMPS
        );


    }


    public Message createMessageFromJSON(String message) {

        Message messageDefinition;

        try {

            messageDefinition = objectMapper.readValue(
                    message,
                    Message.class
            );

            // Retain the original incoming JSON internally.
            messageDefinition.setFullStringOriginalMessage(message);

        } catch (JsonProcessingException jsonExe) {

            // Plain text commands intentionally fall back to CommandProcessor.
            // Treat decode failures as trace-level diagnostics, not file errors
            // on every command request.
            log.debug(() ->
                    "[MessageParser][createMessageFromJSON] Input is not a JSON message; routing as command text"
                            + " | messageLength=" + (message == null ? -1 : message.length())
                            + " | cause=" + jsonExe.getClass().getSimpleName()
            );

            messageDefinition = new Message(
                    MessageType.UNKNOWN,
                    ProcessType.UNKNOWN,
                    Operation.UNKNOWN,
                    NullNode.instance,
                    message,
                    null
            );
        }

        if (messageDefinition.getMessageType() == MessageType.UNKNOWN) {
            attachError(messageDefinition, "JSON_DECODING_ERROR", "Unable to parse incoming message");
        }
        return messageDefinition;
    }


    public String createJSONFromMessage(Message message) {

        String jsonMessage = "";

        try {

            jsonMessage = objectMapper.writeValueAsString(message);

        } catch (JsonProcessingException jsonExe) {

            FileLogger.log(
                    "[createJSONFromMessage] Error while parsing: "
                            + jsonExe.getMessage()
            );

            try {

                Message fallbackMessage = new Message(
                        MessageType.UNKNOWN,
                        processType,
                        Operation.UNKNOWN,
                        NullNode.instance,
                        message.getFullStringOriginalMessage(),
                        null
                );

                attachError(fallbackMessage, "JSON_ENCODING_ERROR", "Unable to serialize outgoing message");
                jsonMessage = objectMapper.writeValueAsString(fallbackMessage);

            } catch (JsonProcessingException ignored) {

                FileLogger.log(
                        "[createJSONFromMessage] Error while parsing, fallback failed..."
                );
            }
        }

        return jsonMessage;
    }


    public <T> T extractPayloadField(
            Message message,
            String fieldName,
            Class<T> targetType) throws JsonProcessingException {

        JsonNode payload = message.getPayload();

        if (payload == null || payload.isNull()) {
            return null;
        }

        JsonNode fieldValue = payload.get(fieldName);

        if (fieldValue == null || fieldValue.isNull()) {
            return null;
        }

        return objectMapper.treeToValue(fieldValue, targetType);
    }

    public Message createErrorResponse(
            UUID correlationId,
            String errorCode,
            String errorDescription
    ) {

        Objects.requireNonNull(correlationId, "Correlation ID cannot be null");

        Objects.requireNonNull(errorCode, "Error code cannot be null");

        Objects.requireNonNull(errorDescription, "Error description cannot be null");


        Message response = createResponse(correlationId, NullNode.instance);

        return attachError(response, errorCode, errorDescription);
    }

    public Message attachError(Message message, String code, String description) {

        Error error = new Error();

        error.setErrorCode(code);
        error.setErrorMessage(description);
        message.setError(error);

        message.setStatus("FAILED");

        message.setResponseComplete(true);

        return message;
    }

    public Message createResponse(UUID correlationId, Object payload) {
        Objects.requireNonNull(correlationId, "Correlation ID cannot be null");
        return new Message(MessageType.RESPONSE, processType, Operation.INFO,
                toPayload(payload), null, null, correlationId);
    }

    public <T> Message putPayloadField(
            Message message,
            String fieldName,
            T fieldValue
    ) {

        Objects.requireNonNull(message, "Message cannot be null");
        Objects.requireNonNull(fieldName, "Field name cannot be null");

        if (fieldName.isBlank()) {
            throw new IllegalArgumentException(
                    "Field name cannot be blank"
            );
        }

        JsonNode existingPayload = message.getPayload();

        ObjectNode payload;

        if (existingPayload == null || existingPayload.isNull()) {

            payload = objectMapper.createObjectNode();

        } else if (existingPayload.isObject()) {

            payload = ((ObjectNode) existingPayload).deepCopy();

        } else {

            throw new IllegalArgumentException(
                    "Message payload must be a JSON object"
            );
        }

        JsonNode value = fieldValue == null
                ? NullNode.instance
                : objectMapper.valueToTree(fieldValue);

        payload.set(fieldName, value);

        message.setPayload(payload);

        return message;
    }

    public Message createEmptyMessage() {
        return new Message(
                MessageType.UNKNOWN,
                processType,
                Operation.UNKNOWN,
                objectMapper.createObjectNode(),
                null,
                null
        );
    }

    public JsonNode convertToJsonNode(Object value) {

        if (value == null) {
            return NullNode.instance;
        }

        try {

            return objectMapper.valueToTree(value);

        } catch (IllegalArgumentException exception) {

            FileLogger.log(
                    "[convertToJsonNode] Serialization failed: "
                            + exception.getMessage()
            );

            throw exception;
        }
    }

    public <T> JsonNode toPayload(T object) {

        Objects.requireNonNull(
                object,
                "Payload object cannot be null"
        );

        return objectMapper.valueToTree(object);
    }

    public <T> T fromPayload(
            JsonNode payload,
            Class<T> payloadType
    ) throws JsonProcessingException {

        Objects.requireNonNull(
                payload,
                "Payload cannot be null"
        );

        Objects.requireNonNull(
                payloadType,
                "Payload type cannot be null"
        );

        return objectMapper.treeToValue(
                payload,
                payloadType
        );
    }

    protected ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
