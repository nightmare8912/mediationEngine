package org.prats.processors;

import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.enums.TCPConnectionMode;
import org.prats.definitions.interfaces.ControlChannelCommunicator;
import org.prats.definitions.interfaces.ControlMessageHandler;
import org.prats.definitions.interfaces.ReplyChannel;
import org.prats.definitions.interfaces.TCPProcessor;
import org.prats.definitions.records.TCPPeer;
import org.prats.loggers.FileLogger;
import org.prats.parsers.MessageParser;
import org.prats.registries.TCPControlRegistry;
import org.prats.replyChannels.NewTCPReplyChannel;
import org.prats.services.tcpService.NewTCPService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

public class NewTCPControlProcessor implements TCPProcessor, ControlChannelCommunicator {

    private static final long CORRELATION_TIMEOUT_MILLIS = 120_000L;

    private final TCPConnectionMode connectionMode;
    private final MessageParser messageParser;

    private final List<TCPPeer> configuredPeers;

    private final TCPControlRegistry tcpControlRegistry =
            new TCPControlRegistry();

    private final Map<String, ClientConnection> activePeerConnections =
            new ConcurrentHashMap<>();

    private NewTCPService tcpService;
    private ControlMessageHandler controlMessageHandler;


    public NewTCPControlProcessor(
            TCPConnectionMode tcpConnectionMode,
            List<TCPPeer> configuredPeers,
            MessageParser messageParser
    ) {

        this.connectionMode =
                Objects.requireNonNull(tcpConnectionMode);

        this.messageParser =
                Objects.requireNonNull(messageParser);

        this.configuredPeers = configuredPeers;
    }


    public void setTcpService(NewTCPService tcpService) {

        this.tcpService = Objects.requireNonNull(tcpService);
    }


    public void setControlMessageHandler(ControlMessageHandler controlMessageHandler) {

        this.controlMessageHandler = Objects.requireNonNull(controlMessageHandler);
    }



    public void registerPeerConnection(
            String peerId,
            ClientConnection connection
    ) {

        Objects.requireNonNull(peerId);
        Objects.requireNonNull(connection);

        ClientConnection existing =
                activePeerConnections.putIfAbsent(peerId, connection);

        if (existing != null && existing != connection) {

            throw new IllegalStateException(
                    "Connection already registered for peer: " + peerId
            );
        }
    }


    public void removePeerConnection(
            String peerId,
            ClientConnection connection
    ) {

        activePeerConnections.remove(peerId, connection);
    }


    private void registerHandshake(Message handshake, ClientConnection clientConnection) {
        activePeerConnections.put(
                handshake.getProcessName(),
                clientConnection
        );
    }


    @Override
    public void processIncoming(
            String rawMessage,
            ClientConnection clientConnection
    ) {

        if (rawMessage == null || rawMessage.isBlank()) {
            return;
        }

        Objects.requireNonNull(clientConnection);

        Message incomingMessage;

        try {

            incomingMessage = Objects.requireNonNull(
                    messageParser.createMessageFromJSON(rawMessage),
                    "Parsed control message cannot be null"
            );

        } catch (Exception exception) {

            FileLogger.log(
                    "[TCPControlProcessor] [processIncoming] "
                            + "Message parsing failed: "
                            + exception.getMessage()
            );

            return;
        }

        try {

            switch (Objects.requireNonNull(
                    incomingMessage.getMessageType(),
                    "Message type cannot be null"
            )) {

                case TCP_HANDSHAKE -> {
                    registerHandshake(
                            incomingMessage,
                            clientConnection
                    );
                }

                case REQUEST -> {

                    UUID correlationId =
                            Objects.requireNonNull(
                                    incomingMessage.getCorrelationId(),
                                    "Request correlation ID cannot be null"
                            );

                    ReplyChannel replyChannel =
                            new NewTCPReplyChannel(
                                    this,
                                    clientConnection
                            );

                    tcpControlRegistry.add(
                            correlationId,
                            replyChannel
                    );

                    try {

                        controlMessageHandler.onRequest(
                                incomingMessage
                        );

                    } catch (Exception exception) {

                        tcpControlRegistry.removeAndGet(correlationId);

                        FileLogger.log(
                                "[TCPControlProcessor] Request handler failed: "
                                        + exception.getMessage()
                        );

                        processOutgoing(
                                messageParser.createErrorResponse(
                                        correlationId,
                                        "REQUEST_PROCESSING_FAILED",
                                        exception.getMessage()
                                ),
                                clientConnection
                        );
                    }
                }

                case RESPONSE ->
                        controlMessageHandler.onResponse(incomingMessage);

                case HEARTBEAT ->
                        controlMessageHandler.onHeartbeat(incomingMessage);

                case INFO ->
                        controlMessageHandler.onInfo(incomingMessage);

                default -> {

                    FileLogger.log(
                            "[TCPControlProcessor] Unsupported message type: "
                                    + incomingMessage.getMessageType()
                    );

                    processOutgoing(
                            messageParser.createErrorResponse(
                                    incomingMessage.getCorrelationId(),
                                    "UNKNOWN_CONTROL",
                                    "Unsupported control message type."
                            ),
                            clientConnection
                    );
                }
            }

        } catch (Exception exception) {

            FileLogger.log(
                    "[TCPControlProcessor] [processIncoming] "
                            + "Control message processing failed: "
                            + exception.getMessage()
            );
        }
    }



    @Override
    public String serialize(Message message) {

        return messageParser.createJSONFromMessage(
                Objects.requireNonNull(message)
        );
    }



    @Override
    public CompletionStage<Void> processOutgoing(
            Message outgoing,
            ClientConnection clientConnection
    ) {

        Objects.requireNonNull(outgoing);
        Objects.requireNonNull(clientConnection);

        try {

            clientConnection.sendMessage(
                    serialize(outgoing)
            );

            return CompletableFuture.completedFuture(null);

        } catch (IOException exception) {

            FileLogger.log(
                    "[TCPControlProcessor] [processOutgoing] "
                            + "Socket write failed: " + exception
            );

            return CompletableFuture.failedFuture(exception);
        }
    }



    @Override
    public void informShutdown() {

        FileLogger.log(
                "[TCPControlProcessor] [informShutdown] inform shutdown called by tcp service"
        );

        // A zero timeout removes every currently registered correlation.
        for (TCPControlRegistry.ExpiredCorrelation expired :
                tcpControlRegistry.removeExpired(0)) {

            try {

                Message failure = messageParser.createErrorResponse(
                        expired.correlationId(),
                        "CONTROL_PROCESSOR_SHUTDOWN",
                        "Control processor is shutting down."
                );

                expired.replyChannel().sendReply(failure)
                        .whenComplete((result, exception) -> {

                            if (exception != null) {

                                FileLogger.log(
                                        "[TCPControlProcessor] Unable to send "
                                                + "shutdown failure reply: "
                                                + exception.getMessage()
                                );
                            }
                        });

            } catch (Exception exception) {

                FileLogger.log(
                        "[TCPControlProcessor] Shutdown notification failed: "
                                + exception.getMessage()
                );
            }
        }

        activePeerConnections.clear();
    }

    @Override
    public void cleanupCorrelations() {
        cleanupExpiredCorrelations();
    }

    private Message createHandshakeMessage() {
        Message handshake = messageParser.createEmptyMessage();
        handshake.setMessageType(MessageType.TCP_HANDSHAKE);
        return handshake;
    }

    private void sendHandshake(ClientConnection connection) {
        processOutgoing(
                createHandshakeMessage(),
                connection
        );
    }

    @Override
    public synchronized void reconcilePeers() {

        if (connectionMode == TCPConnectionMode.SERVER_ONLY)
            return;

        for (TCPPeer tcpPeer: configuredPeers) {
            if (activePeerConnections.get(tcpPeer.processId()) == null) {
                ClientConnection connection = tcpService.attemptConnection(tcpPeer);
                if (connection != null) {
                    try {
                        sendHandshake(connection);
                    } catch (Exception exception) {
                        FileLogger.log(
                                "[TCPControlProcessor][reconcilePeers], connection established but not able to send"
                                + "handshake to peer: " + tcpPeer.processId() + "on port: " + tcpPeer.port()
                        );
                    }
                    activePeerConnections.put(
                            tcpPeer.processId(),
                            connection
                    );
                }
                else
                    activePeerConnections.remove(
                            tcpPeer.processId()
                    );
            }
        }
    }


    public CompletionStage<Void> processOutgoing(Message outgoing) {

        Objects.requireNonNull(outgoing);


        if (outgoing.getMessageType() == MessageType.RESPONSE) {

            UUID correlationId = outgoing.getCorrelationId();

            if (correlationId == null) {

                return CompletableFuture.failedFuture(
                        new IllegalArgumentException(
                                "Response correlation ID cannot be null"
                        )
                );
            }

            ReplyChannel replyChannel;

            if (outgoing.isResponseComplete()) {

                replyChannel =
                        tcpControlRegistry.removeAndGet(correlationId);

            } else {

                replyChannel =
                        tcpControlRegistry.get(correlationId);

                tcpControlRegistry.resetTimeout(correlationId);
            }

            if (replyChannel == null) {

                return CompletableFuture.failedFuture(
                        new IllegalStateException(
                                "No pending reply channel for correlation ID: "
                                        + correlationId
                        )
                );
            }

            return replyChannel.sendReply(outgoing);
        }



        String recipientId = outgoing.getRecipientID();

        if (recipientId == null || recipientId.isBlank()) {

            return CompletableFuture.failedFuture(
                    new IllegalArgumentException(
                            "Outgoing message recipient ID is missing"
                    )
            );
        }

        ClientConnection clientConnection =
                activePeerConnections.get(recipientId);

        if (clientConnection == null) {

            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "No active connection for peer: " + recipientId
                    )
            );
        }

        if (tcpService == null) {

            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "TCP service has not been initialized"
                    )
            );
        }

        return tcpService.writeMessage(
                serialize(outgoing),
                clientConnection
        );
    }



    public CompletionStage<Void> processOutgoing(
            Message outgoing,
            ReplyChannel replyChannel
    ) {

        Objects.requireNonNull(outgoing);
        Objects.requireNonNull(replyChannel);

        return replyChannel.sendReply(outgoing);
    }


    public void cleanupExpiredCorrelations() {

        for (TCPControlRegistry.ExpiredCorrelation expired :
                tcpControlRegistry.removeExpired(
                        CORRELATION_TIMEOUT_MILLIS
                )) {

            try {

                Message failure = messageParser.createErrorResponse(
                        expired.correlationId(),
                        "REQUEST_TIMEOUT",
                        "Control request timed out."
                );

                expired.replyChannel().sendReply(failure)
                        .whenComplete((result, exception) -> {

                            if (exception != null) {

                                FileLogger.log(
                                        "[TCPControlProcessor] Timeout reply "
                                                + "delivery failed: "
                                                + exception.getMessage()
                                );
                            }
                        });

            } catch (Exception exception) {

                FileLogger.log(
                        "[TCPControlProcessor] Timeout cleanup failed: "
                                + exception.getMessage()
                );
            }
        }
    }

    @Override
    public CompletionStage<Void> sendRequest(Message request) {
        return processOutgoing(
                request
        );
    }

    public CompletionStage<Void> sendRequest(
            Message request,
            ReplyChannel replyChannel
    ) {

        UUID correlationId = request.getCorrelationId();

        tcpControlRegistry.add(
                correlationId,
                replyChannel
        );

        return processOutgoing(request)
                .whenComplete((ignored, error) -> {
                    if (error != null) {
                        tcpControlRegistry.removeAndGet(correlationId);
                    }
                });

    }


    @Override
    public void sendResponse(Message response) {
        processOutgoing(
                response
        );
    }

    @Override
    public void disconnect(String connectionName) {
        ClientConnection clientConnection = activePeerConnections.remove(connectionName);
        if (clientConnection != null)
            onConnectionClosed(clientConnection);
    }
}