package org.prats.messageHandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.prats.definitions.MessageDefinition;
import org.prats.definitions.MessageTypeDefinition;
import org.prats.definitions.OperationDefinition;
import org.prats.definitions.ProcessTypeDefinition;

import java.util.UUID;


public class MessageUtils {
    public MessageDefinition createAcknowledgementMessage(
            ProcessTypeDefinition processTypeDefinition,
            MessageTypeDefinition messageTypeDefinition,
            OperationDefinition operationDefinition,
            UUID correlationID
    ) {
        return new MessageDefinition (
                messageTypeDefinition,
                processTypeDefinition,
                operationDefinition,
                new ObjectMapper().valueToTree("{}"),
                correlationID
        );
    }

}
