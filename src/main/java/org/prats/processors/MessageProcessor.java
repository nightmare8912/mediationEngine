package org.prats.processors;

import org.prats.definitions.Message;
import org.prats.forwarders.DatabaseForwarder;

public class MessageProcessor {

    private final DatabaseForwarder databaseForwarder;

    public MessageProcessor(
            DatabaseForwarder databaseForwarder
    ) {
        this.databaseForwarder = databaseForwarder;
    }

    public void process(Message message) {
        switch (message.getOperation()) {
            case FETCH, SEARCH, CREATE, UPDATE, DELETE  -> databaseForwarder.forward(message);
        }
    }
}
