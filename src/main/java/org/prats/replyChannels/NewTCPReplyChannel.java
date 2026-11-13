package org.prats.replyChannels;

import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.ReplyChannel;
import org.prats.definitions.interfaces.TCPProcessor;
import java.util.concurrent.CompletionStage;

public class NewTCPReplyChannel implements ReplyChannel {

    private final TCPProcessor tcpProcessor;
    private final ClientConnection clientConnection;

    public NewTCPReplyChannel(
            TCPProcessor tcpProcessor,
            ClientConnection clientConnection
    ) {
        this.tcpProcessor       = tcpProcessor;
        this.clientConnection   = clientConnection;
    }

    @Override
    public CompletionStage<Void> sendReply(Message response) {
        return tcpProcessor.processOutgoing(
                response,
                clientConnection
        );
    }
}
