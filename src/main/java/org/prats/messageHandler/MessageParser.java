package org.prats.messageHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.prats.definitions.MessageDefinition;
import org.prats.definitions.ProcessTypeDefinition;
import org.prats.definitions.MessageTypeDefinition;
import org.prats.definitions.OperationDefinition;
import org.prats.loggers.ShellLogger;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.databind.SerializationFeature;

public class MessageParser {

    private final ObjectMapper objectMapper;
    private final ProcessTypeDefinition processTypeDefinition;

    private final JsonNode errorPayload;

    public MessageParser(ProcessTypeDefinition processTypeDefinition) {

        this.processTypeDefinition = processTypeDefinition;

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

    public MessageDefinition createMessageFromJSON(String message) {
        MessageDefinition messageDefinition = null;
        try {
            messageDefinition = objectMapper.readValue(
                    message,
                    MessageDefinition.class
            );
        } catch (JsonProcessingException jsonExe) {
            ShellLogger.log("Error while parsing message: " + message + "\n" + jsonExe.getMessage());
            messageDefinition = new MessageDefinition(
                    MessageTypeDefinition.RESP,
                    processTypeDefinition,
                    OperationDefinition.UNKNOWN,
                    errorPayload
            );
            }
        return messageDefinition;
    }

    public String createJSONFromMessage(MessageDefinition message) {
        String jsonMessage = "";
        try {
            jsonMessage = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException jsonExe) {
            ShellLogger.log(
                    "[createJSONFromMessage] Error while parsing"
                    + jsonExe.getMessage()
            );
            try {
                jsonMessage = objectMapper.writeValueAsString(new MessageDefinition(
                        MessageTypeDefinition.ERR,
                        processTypeDefinition,
                        OperationDefinition.UNKNOWN,
                        errorPayload
                ));
            } catch (JsonProcessingException ignored) {
                // throw new RuntimeException(e);
                ShellLogger.log(
                        "[createJSONFromMessage] Error while parsing, fallback failed..."
                );
            }
        }
        return jsonMessage;
    }
}
