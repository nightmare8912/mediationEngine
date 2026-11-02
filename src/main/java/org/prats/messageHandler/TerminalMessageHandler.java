package org.prats.messageHandler;

import org.prats.definitions.MessageEntity;

public class TerminalMessageHandler implements MessageHandler {

    private MessageProcessor messageProcessor;
    public TerminalMessageHandler(MessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;
    }

    @Override
    public void handleMessage(
            MessageEntity requestMessageEntity
    ) {

    }
}
