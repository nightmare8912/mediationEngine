package org.prats.inputHandler.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.prats.definitions.Message;
import org.prats.definitions.MessageType;
import org.prats.definitions.Operation;
import org.prats.definitions.ProcessType;

import java.util.UUID;


public class MessageUtils {
    public Message createAcknowledgementMessage(
            ProcessType processType,
            MessageType messageType,
            Operation operation,
            UUID correlationID
    ) {
//        return new Message(
//                messageType,
//                processType,
//                operation,
//                new ObjectMapper().valueToTree("{}"),
//                correlationID
//        );

        return null;
    }

}
