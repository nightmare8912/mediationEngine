package org.prats.messageHandler;

import org.prats.definitions.MessageEntity;

public interface MessageHandler {
    public void handleMessage(
            MessageEntity messageEntity
    );
}
