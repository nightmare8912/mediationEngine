package org.prats.messageHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.prats.definitions.MessageDefinition;

public class PingResponder {

    private static MessageParser messageParser;

    public PingResponder() {}

    public static String createMessage(MessageDefinition messageDefinition)
            throws JsonProcessingException {

        if (messageParser == null) messageParser = new MessageParser();

        return messageParser.createJSONFromMessage(
                new MessageDefinition(
                        MessageType.ACK,
                        messageDefinition.getMessageSource(),
                        "ACK",
                        messageDefinition.getCorrelationId()
                )
        );
    }
}
