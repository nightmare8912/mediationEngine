package org.prats.messageHandler;

import com.fasterxml.jackson.databind.JsonNode;
import org.prats.definitions.Message;
import org.prats.definitions.MessageEntity;
import org.prats.definitions.MessageType;
import org.prats.definitions.Operation;

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
        Message responseMessage = messageUtils.createAcknowledgementMessage(
               messageProcessor.getProcessTypeDefinition(),
                MessageType.ACK,
                Operation.UNKNOWN,
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
