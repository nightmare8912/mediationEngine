package org.prats.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.enums.TCPConnectionMode;
import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.definitions.interfaces.TCPProcessor;
import org.prats.definitions.records.TCPPeer;
import org.prats.loggers.FileLogger;
import org.prats.parsers.MessageParser;
import org.prats.replyChannels.NewTCPReplyChannel;
import org.prats.services.tcpService.NewTCPService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

public class NewTCPApplicationProcessor implements TCPProcessor {

    private final MessageDispatcher messageDispatcher;
    private final MessageParser messageParser;

    private final TCPConnectionMode connectionMode
            = TCPConnectionMode.SERVER_AND_PEERS;

    private final List<TCPPeer> configuredPeers;

    private final Map<String, ClientConnection> activePeerConnections =
            new ConcurrentHashMap<>();

    private NewTCPService tcpService;

    public NewTCPApplicationProcessor(
            List<TCPPeer> configuredPeers,
            MessageDispatcher messageDispatcher,
            MessageParser messageParser
    ) {
        this.configuredPeers        = configuredPeers;
        this.messageDispatcher      = messageDispatcher;
        this.messageParser          = messageParser;
    }

    public void setTcpService(NewTCPService tcpService) {
        this.tcpService = tcpService;
    }

//    private void registerHandshake(Message handshake, ClientConnection clientConnection) {
//        activePeerConnections.put(
//                handshake.getProcessName(),
//                clientConnection
//        );
//    }

    @Override
    public void processIncoming(String rawMessage, ClientConnection clientConnection) {

        Message incomingMessage =
                messageParser.createMessageFromJSON(rawMessage);

//        if (incomingMessage.getMessageType() == MessageType.TCP_HANDSHAKE) {
//            registerHandshake(incomingMessage, clientConnection);
//            return;
//        }

        messageDispatcher.dispatchRequest(
                messageParser.createMessageFromJSON(rawMessage),
                new NewTCPReplyChannel(
                        this,
                        clientConnection
                )
        );

    }

    public CompletionStage<Void> processOutgoing(
            Message outgoing,
            ClientConnection clientConnection
    ) {
        if (outgoing.getOperation() == Operation.COMMAND) {
            try {
                return tcpService.writeMessage(
                        messageParser.extractPayloadField(
                                outgoing,
                                "body",
                                String.class
                        ),
                        clientConnection
                );
            } catch (JsonProcessingException jsonProcessingException) {
                FileLogger.log(
                        "[][]processOutgoing: error: " + jsonProcessingException
                );
            }
        }


        return tcpService.writeMessage(
                serialize(outgoing),
                clientConnection
        );
    }

    @Override
    public void informShutdown() {
        // not required
    }

    @Override
    public void cleanupCorrelations() {
        // not required
    }

    @Override
    public synchronized void reconcilePeers() {

        if (connectionMode == TCPConnectionMode.SERVER_ONLY)
            return;

        for (TCPPeer tcpPeer: configuredPeers) {
            if (activePeerConnections.get(tcpPeer.processId()) != null) {
                ClientConnection connection = tcpService.attemptConnection(tcpPeer);
                if (connection != null)
                    activePeerConnections.put(
                            tcpPeer.processId(),
                            connection
                    );
                else
                    activePeerConnections.remove(
                            tcpPeer.processId()
                    );
            }
        }
    }

    @Override
    public String serialize(Message message) {
        return messageParser.createJSONFromMessage(
                message
        );
    }
}
