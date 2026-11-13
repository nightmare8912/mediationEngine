
package org.prats.parsers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.enums.ProcessType;
import org.prats.loggers.FileLogger;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class MessageParser {

    private final ObjectMapper objectMapper;
    private final ProcessType processType;

    private final JsonNode errorPayload;


    public MessageParser(ProcessType processType) {

        this.processType = processType;

        this.objectMapper = new ObjectMapper();

        objectMapper.registerModule(new JavaTimeModule());

        objectMapper.disable(
                SerializationFeature.WRITE_DATES_AS_TIMESTAMPS
        );

        ObjectNode error = objectMapper.createObjectNode();
        error.put("errorCode", "JSON_DECODING_ERROR");
        error.put("error", "Unable to parse incoming message");

        this.errorPayload = error;
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

            FileLogger.log(
                    "Error while parsing message: "
                            + message + "\n"
                            + jsonExe.getMessage()
            );

            messageDefinition = new Message(
                    MessageType.UNKNOWN,
                    ProcessType.UNKNOWN,
                    Operation.UNKNOWN,
                    errorPayload,
                    message,
                    null
            );
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
                        errorPayload,
                        message.getFullStringOriginalMessage(),
                        null
                );

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

        ObjectNode payload = objectMapper.createObjectNode();

        payload.put("errorCode", errorCode);
        payload.put("errorDescription", errorDescription);

        return new Message(
                MessageType.RESPONSE,
                processType,
                Operation.INFO,
                payload,
                null,
                null,
                correlationId
        );
    }

    public Message createResponse(
            UUID correlationId,
            Map<String, String> messageValues
    ) {
        Objects.requireNonNull(correlationId, "Correlation ID cannot be null");
        Objects.requireNonNull(messageValues, "Message cannot be null");

        ObjectNode payload = objectMapper.createObjectNode();

        for (Map.Entry<String, String> field: messageValues.entrySet()) {
            payload.put(field.getKey(), field.getValue());
        }

        return new Message(
                MessageType.RESPONSE,
                processType,
                Operation.INFO,
                payload,
                null,
                null,
                correlationId
        );
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

    protected ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
