package org.prats.messageHandler;

import org.prats.platform.tcpService.ClientConnection;

public interface MessageSender {
    void sendMessage(String message, ClientConnection connection);
}
