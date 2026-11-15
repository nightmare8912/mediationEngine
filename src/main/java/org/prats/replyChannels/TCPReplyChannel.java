package org.prats.replyChannels;

import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.ReplyChannel;
import org.prats.definitions.interfaces.TCPProcessor;
import org.prats.loggers.EngineLogger;

import java.util.concurrent.CompletionStage;

public class TCPReplyChannel implements ReplyChannel {

    private static final EngineLogger log =
            EngineLogger.getLogger(TCPReplyChannel.class);

    private final TCPProcessor tcpProcessor;
    private final ClientConnection clientConnection;

    public TCPReplyChannel(
            TCPProcessor tcpProcessor,
            ClientConnection clientConnection
    ) {
        this.tcpProcessor = tcpProcessor;
        this.clientConnection = clientConnection;

        log.debug(() ->
                "[TCPReplyChannel][constructor] TCP reply channel initialized"
                        + " | tcpProcessor="
                        + (tcpProcessor == null
                        ? "null"
                        : tcpProcessor.getClass().getSimpleName())
                        + " | clientConnection="
                        + (clientConnection == null
                        ? "null"
                        : clientConnection.getClass().getSimpleName())
        );
    }

    @Override
    public boolean isClosed() { return clientConnection.getSocket().isClosed(); }
    @Override
    public boolean belongsTo(ClientConnection connection) { return clientConnection == connection; }

    @Override
    public CompletionStage<Void> sendReply(Message response) {

        log.debug(() ->
                "[TCPReplyChannel][sendReply] Dispatching outgoing response"
                        + " | responsePresent=" + (response != null)
                        + " | clientConnectionPresent=" + (clientConnection != null)
        );

        return tcpProcessor.processOutgoing(
                response,
                clientConnection
        );
    }
}
