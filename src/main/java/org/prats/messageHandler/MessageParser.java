package org.prats.messageHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.prats.definitions.MessageDefinition;
import org.prats.loggers.ShellLogger;

public class MessageParser {

    private final ObjectMapper objectMapper;

    public MessageParser() {
        this.objectMapper = new ObjectMapper();
    }

    public MessageDefinition createMessageFromJSON(String message) {
        MessageDefinition messageDefinition;
        try {
            messageDefinition =  objectMapper.readValue(
                    message,
                    MessageDefinition.class
            );
        } catch (Exception exe) {
            ShellLogger.log(exe.getMessage());
            messageDefinition =  new MessageDefinition(
                    MessageType.UNKNOWN,
                    MessageSource.UNKNOWN,
                    ""
            );
        }
        return messageDefinition;
    }

    public String createJSONFromMessage(MessageDefinition message)
            throws JsonProcessingException {

        return objectMapper.writeValueAsString(message);
    }
}
