package org.prats.replyChannels;

import org.prats.definitions.ClientConnection;
import org.prats.services.tcpService.TCPService;

import java.util.concurrent.CompletionStage;

public class TCPReplyChannel implements ReplyChannel {

    private final ClientConnection clientConnection;
    private final TCPService tcpService;

    public TCPReplyChannel(ClientConnection clientConnection, TCPService tcpService) {
        this.clientConnection   = clientConnection;
        this.tcpService         = tcpService;
    }

    @Override
    public synchronized CompletionStage<Void> sendReply(String reply) throws Exception {
        return tcpService.sendReply(reply, clientConnection);
    }
}
