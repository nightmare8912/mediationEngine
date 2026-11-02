package org.prats.messageHandler;

import org.prats.definitions.ClientConnection;
import org.prats.services.tcpService.TCPService;

public class TCPReplyChannel implements ReplyChannel{
    public final ClientConnection clientConnection;
    public final TCPService tcpService;

    public TCPReplyChannel(
            ClientConnection clientConnection,
            TCPService tcpService
    ) {
        this.clientConnection = clientConnection;
        this.tcpService                 = tcpService;
    }

    @Override
    public void sendMessage(String message) {
        tcpService.sendMessage(
                message,
                clientConnection
        );
    }
}
