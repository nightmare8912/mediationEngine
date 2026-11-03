package org.prats.inputHandler.messageForwarders;

import org.prats.definitions.MessageEntity;
import org.prats.inputHandler.processors.MessageProcessor;
import org.prats.services.Service;

public class ECMessageForwarder implements MessageForwarder {

    private MessageProcessor messageProcessor;
    public ECMessageForwarder(MessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;
    }


    @Override
    public void forwardMessage(MessageEntity messageEntity, Service service) {

    }
}
