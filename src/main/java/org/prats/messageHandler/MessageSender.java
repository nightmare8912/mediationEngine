package org.prats.messageHandler;

import org.prats.definitions.ClientConnection;

public interface MessageSender {
    void sendMessage(String message, ClientConnection connection);
}
