package org.prats.messageHandler;

import com.fasterxml.jackson.databind.JsonNode;
import org.prats.definitions.MessageDefinition;
import org.prats.definitions.MessageEntity;
import org.prats.definitions.MessageTypeDefinition;
import org.prats.definitions.OperationDefinition;

public class MiscellaneousMessageHandler implements MessageHandler {

    private MessageProcessor messageProcessor;
    private MessageUtils messageUtils;

    private JsonNode json;

    public MiscellaneousMessageHandler(MessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;
        this.messageUtils     = new MessageUtils();
    }

    @Override
    public void handleMessage(
            MessageEntity requestMessageEntity
    ) {
        MessageDefinition responseMessage = messageUtils.createAcknowledgementMessage(
               messageProcessor.getProcessTypeDefinition(),
                MessageTypeDefinition.ACK,
                OperationDefinition.UNKNOWN,
                requestMessageEntity.getMessageDefinition().getCorrelationId()
        );
        messageProcessor.sendMessage(
                new MessageEntity(
                        responseMessage,
                        requestMessageEntity.getReplyChannel()
                )
        );
    }
}
