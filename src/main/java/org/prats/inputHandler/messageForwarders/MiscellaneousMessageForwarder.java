package org.prats.inputHandler.messageForwarders;

import com.fasterxml.jackson.databind.JsonNode;
import org.prats.definitions.Message;
import org.prats.definitions.MessageEntity;
import org.prats.definitions.MessageType;
import org.prats.definitions.Operation;
import org.prats.inputHandler.utils.MessageUtils;
import org.prats.inputHandler.processors.MessageProcessor;
import org.prats.services.Service;

public class MiscellaneousMessageForwarder implements MessageForwarder {

    private MessageProcessor messageProcessor;
    private MessageUtils messageUtils;

    private JsonNode json;

    public MiscellaneousMessageForwarder(MessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;
        this.messageUtils     = new MessageUtils();
    }

    @Override
    public void forwardMessage(MessageEntity messageEntity, Service service) {
        Message responseMessage = messageUtils.createAcknowledgementMessage(
                messageProcessor.getProcessTypeDefinition(),
                MessageType.ACK,
                Operation.UNKNOWN,
                messageEntity.getMessage().getCorrelationId()
        );
        messageProcessor.sendMessage(
                new MessageEntity(
                        responseMessage,
                        messageEntity.getReplyChannel(),
                        messageEntity.getOriginalMessage()
                )
        );
    }
}
