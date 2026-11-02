package org.prats.messageHandler;

import org.prats.definitions.MessageEntity;

public class ECMessageHandler implements MessageHandler {

    private MessageProcessor messageProcessor;
    public ECMessageHandler(MessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;
    }

    @Override
    public void handleMessage(
            MessageEntity messageEntity
    ) {

    }
}
