package org.prats.replyChannels;

import org.prats.definitions.*;
import org.prats.services.tcpService.TCPService;

import java.util.concurrent.CompletionStage;

public class TCPReplyChannel implements ReplyChannel {

    private final ClientConnection clientConnection;
    private final TCPService tcpService;

    public TCPReplyChannel(
            ClientConnection clientConnection,
            TCPService tcpService
    ) {
        this.clientConnection = clientConnection;
        this.tcpService = tcpService;
    }

    @Override
    public CompletionStage<Void> sendReply(Message response) {
        return tcpService.writeMessage(
                response,
                clientConnection
        );
    }
}