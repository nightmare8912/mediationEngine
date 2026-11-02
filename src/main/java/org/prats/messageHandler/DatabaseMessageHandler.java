package org.prats.messageHandler;

import org.prats.definitions.MessageEntity;

public class DatabaseMessageHandler implements  MessageHandler {
    private MessageProcessor messageProcessor;
    public DatabaseMessageHandler(MessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;
    }

    @Override
    public void handleMessage(
            MessageEntity messageEntity
    ) {

    }
}
