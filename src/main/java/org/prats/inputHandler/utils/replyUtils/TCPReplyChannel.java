package org.prats.inputHandler.utils.replyUtils;

import org.prats.definitions.ClientConnection;
import org.prats.definitions.MessageEntity;
import org.prats.services.tcpService.TCPService;

public class TCPReplyChannel implements ReplyChannel {
    public final ClientConnection clientConnection;
    public final TCPService tcpService;

    public TCPReplyChannel(
            ClientConnection clientConnection,
            TCPService tcpService
    ) {
        this.clientConnection = clientConnection;
        this.tcpService       = tcpService;
    }

    @Override
    public void sendReply(
            String message
    ) {
        tcpService.sendMessage(
                message,
                clientConnection
        );
    }
}
