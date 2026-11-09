
package org.prats.parsers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.prats.definitions.Message;
import org.prats.definitions.MessageType;
import org.prats.definitions.Operation;
import org.prats.definitions.ProcessType;
import org.prats.loggers.ShellLogger;

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

            ShellLogger.log(
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

            ShellLogger.log(
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

                ShellLogger.log(
                        "[createJSONFromMessage] Error while parsing, fallback failed..."
                );
            }
        }

        return jsonMessage;
    }


    public String extractPayloadField(Message message, String fieldName) {

        JsonNode payload = message.getPayload();

        if (payload == null || payload.isNull()) {
            return null;
        }

        JsonNode fieldValue = payload.get(fieldName);

        if (fieldValue == null || fieldValue.isNull()) {
            return null;
        }

        return fieldValue.asText();
    }


    protected ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
