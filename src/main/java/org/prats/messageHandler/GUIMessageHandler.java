package org.prats.messageHandler;

import org.prats.definitions.MessageEntity;

public class GUIMessageHandler implements MessageHandler {
    private MessageProcessor messageProcessor;
    public GUIMessageHandler(MessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;
    }

    @Override
    public void handleMessage(
            MessageEntity messageEntity
    ) {

    }
}
