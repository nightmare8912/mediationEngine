package org.prats.messageHandler;

import org.prats.definitions.ClientConnectionDefinition;

public interface MessageSender {
    void sendMessage(String message, ClientConnectionDefinition connection);
}
