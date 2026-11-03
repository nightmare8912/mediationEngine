package org.prats.inputHandler.messageForwarders;

import org.prats.definitions.MessageEntity;
import org.prats.inputHandler.handlers.DatabaseMessageHandler;
import org.prats.services.Service;

public class DatabaseMessageForwarder implements MessageForwarder {

    private DatabaseMessageHandler databaseMessageHandler;

    public DatabaseMessageForwarder() {
        this.databaseMessageHandler = new DatabaseMessageHandler();
    }

    @Override
    public void forwardMessage(
            MessageEntity messageEntity,
            Service databaseService
    ) {
        databaseService.submit(messageEntity);
    }

}
