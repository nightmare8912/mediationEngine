
package org.prats.processors;

import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.TCPConnectionMode;
import org.prats.definitions.interfaces.ExternalChanelCommunicator;
import org.prats.definitions.interfaces.TCPMessageHandler;
import org.prats.definitions.interfaces.ReplyChannel;
import org.prats.definitions.interfaces.TCPProcessor;
import org.prats.definitions.records.TCPPeer;
import org.prats.loggers.EngineLogger;
import org.prats.managers.TCPHandshakeManager;
import org.prats.parsers.MessageParser;
import org.prats.registries.TCPCorrelationRegistry;
import org.prats.replyChannels.TCPReplyChannel;
import org.prats.services.tcpService.TCPService;

import java.net.Socket;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class TCPProtocolProcessor
        implements TCPProcessor, ExternalChanelCommunicator {

    private static final EngineLogger log =
            EngineLogger.getLogger(TCPProtocolProcessor.class);

    // ---------------------------------------------------------
    // CONSTANTS
    // ---------------------------------------------------------

    private static final long CORRELATION_TIMEOUT_MILLIS =
            120_000L;

    // ---------------------------------------------------------
    // PROCESSOR CONFIGURATION
    // ---------------------------------------------------------

    private final TCPConnectionMode connectionMode;

    private final MessageParser messageParser;

    private final List<TCPPeer> configuredPeers;

    // ---------------------------------------------------------
    // REQUEST / RESPONSE CORRELATION
    // ---------------------------------------------------------

    /*
     * This registry is unrelated to TCP handshakes.
     *
     * It retains the original responsibility of associating
     * control requests with their reply channels.
     */
    private final TCPCorrelationRegistry tcpCorrelationRegistry =
            new TCPCorrelationRegistry();

    // ---------------------------------------------------------
    // HANDSHAKE MANAGEMENT
    // ---------------------------------------------------------

    /*
     * Handles:
     *
     * 1. Handshake message creation.
     * 2. Handshake request/response processing.
     * 3. Pending handshake registration.
     * 4. Active peer registration.
     * 5. Handshake timeouts.
     * 6. Peer connection cleanup.
     *
     * The original control-channel handshake requirement
     * remains unchanged.
     */
    private final TCPHandshakeManager handshakeManager;

    // ---------------------------------------------------------
    // DEPENDENCIES
    // ---------------------------------------------------------

    private volatile TCPService tcpService;

    private volatile TCPMessageHandler tcpMessageHandler;

    // ---------------------------------------------------------
    // CONSTRUCTOR
    // ---------------------------------------------------------

    public TCPProtocolProcessor(
            TCPConnectionMode tcpConnectionMode,
            List<TCPPeer> configuredPeers,
            MessageParser messageParser
    ) {

        this.connectionMode = Objects.requireNonNull(
                tcpConnectionMode,
                "TCP connection mode cannot be null"
        );

        this.messageParser = Objects.requireNonNull(
                messageParser,
                "Message parser cannot be null"
        );

        this.configuredPeers = List.copyOf(
                Objects.requireNonNull(
                        configuredPeers,
                        "Configured peer list cannot be null"
                )
        );

        /*
         * The handshake manager uses the same peer list
         * and the same direct outgoing message mechanism
         * previously implemented in this processor.
         */
        this.handshakeManager =
                new TCPHandshakeManager(
                        this.configuredPeers,
                        this.messageParser,
                        (message, connection) ->
                                processOutgoing(message, connection)
                );

        log.info(() ->
                "[TCPProtocolProcessor][constructor] "
                        + "TCP protocol processor initialized"
                        + " | connectionMode=" + connectionMode
                        + " | configuredPeers="
                        + this.configuredPeers.size()
        );
    }

    // ---------------------------------------------------------
    // DEPENDENCY INJECTION
    // ---------------------------------------------------------

    public void setTcpService(
            TCPService tcpService
    ) {

        this.tcpService = Objects.requireNonNull(
                tcpService,
                "TCP service cannot be null"
        );

        log.debug(() ->
                "[TCPProtocolProcessor][setTcpService] "
                        + "TCP service configured"
                        + " | serviceType="
                        + tcpService.getClass().getSimpleName()
        );
    }

    public void setControlMessageHandler(
            TCPMessageHandler tcpMessageHandler
    ) {

        this.tcpMessageHandler = Objects.requireNonNull(
                tcpMessageHandler,
                "TCPMessageHandler cannot be null"
        );

        log.debug(() ->
                "[TCPProtocolProcessor][setControlMessageHandler] "
                        + "TCPMessageHandler configured"
                        + " | handlerType="
                        + tcpMessageHandler.getClass().getSimpleName()
        );
    }

    private TCPMessageHandler requireTCPMessageHandler() {

        return Objects.requireNonNull(
                tcpMessageHandler,
                "Control message handler has not been initialized"
        );
    }

    // ---------------------------------------------------------
    // PEER CONNECTION MANAGEMENT
    // ---------------------------------------------------------

    @Override
    public List<Socket> connectedSockets() {

        return handshakeManager.connectedSockets();
    }

    public boolean isPeerConnected(
            String peerId
    ) {

        return handshakeManager.isPeerConnected(peerId);
    }

    @Override
    public boolean isPeerRegistered(
            String peerId
    ) {

        return handshakeManager.isPeerRegistered(peerId);
    }

    /*
     * Preserve the original public registration API
     * for existing callers.
     *
     * Normal handshake registration is now owned by
     * TCPHandshakeManager.
     */
    public void registerPeerConnection(
            String peerId,
            ClientConnection connection
    ) {

        handshakeManager.registerPeerConnection(
                peerId,
                connection
        );
    }

    public void removePeerConnection(
            String peerId,
            ClientConnection connection
    ) {

        handshakeManager.removePeerConnection(
                peerId,
                connection
        );
    }

    // ---------------------------------------------------------
    // INCOMING MESSAGE PROCESSING
    // ---------------------------------------------------------

    @Override
    public void processIncoming(
            String rawMessage,
            ClientConnection clientConnection
    ) {

        if (rawMessage == null || rawMessage.isBlank()) {

            log.debug(
                    "[TCPProtocolProcessor][processIncoming] "
                            + "Empty control message ignored"
            );

            return;
        }

        Objects.requireNonNull(
                clientConnection,
                "Client connection cannot be null"
        );

        log.debug(() ->
                "[TCPProtocolProcessor][processIncoming] "
                        + "Incoming control message received"
                        + " | messageLength=" + rawMessage.length()
        );

        Message incomingMessage;

        try {

            incomingMessage = Objects.requireNonNull(
                    messageParser.createMessageFromJSON(rawMessage),
                    "Parsed control message cannot be null"
            );

        } catch (Exception exception) {

            log.error(
                    "[TCPProtocolProcessor][processIncoming] "
                            + "Message parsing failed",
                    exception
            );

            return;
        }

        try {

            MessageType messageType =
                    Objects.requireNonNull(
                            incomingMessage.getMessageType(),
                            "Message type cannot be null"
                    );

            log.debug(() ->
                    "[TCPProtocolProcessor][processIncoming] "
                            + "Control message parsed"
                            + " | messageType=" + messageType
                            + " | operation="
                            + incomingMessage.getOperation()
                            + " | processName="
                            + incomingMessage.getProcessName()
                            + " | correlationId="
                            + incomingMessage.getCorrelationId()
            );

            // -------------------------------------------------
            // HANDSHAKE
            // -------------------------------------------------

            /*
             * Delegate the two existing handshake message
             * types to the extracted handshake manager.
             *
             * Handshake processing still happens before
             * normal peer registration validation.
             */
            if (handshakeManager.processHandshake(
                    incomingMessage,
                    clientConnection
            )) {

                log.debug(() ->
                        "[TCPProtocolProcessor][processIncoming] "
                                + "Handshake message handled"
                                + " | messageType=" + messageType
                );

                return;
            }

            // -------------------------------------------------
            // NORMAL CONTROL MESSAGE PROCESSING
            // -------------------------------------------------

            switch (messageType) {

                // ---------------------------------------------
                // REQUEST
                // ---------------------------------------------

                case REQUEST -> {

                    if (!handshakeManager.isRegisteredPeerConnection(
                            incomingMessage,
                            clientConnection
                    )) {

                        log.warn(() ->
                                "[TCPProtocolProcessor][processIncoming] "
                                        + "Request rejected because peer has not completed handshake"
                                        + " | processName="
                                        + incomingMessage.getProcessName()
                                        + " | correlationId="
                                        + incomingMessage.getCorrelationId()
                        );

                        sendProtocolError(
                                incomingMessage,
                                clientConnection,
                                "PEER_NOT_AUTHENTICATED",
                                "Peer handshake has not been completed."
                        );

                        return;
                    }

                    handleIncomingRequest(
                            incomingMessage,
                            clientConnection
                    );
                }

                // ---------------------------------------------
                // RESPONSE
                // ---------------------------------------------

                case RESPONSE -> {

                    if (!handshakeManager.isRegisteredPeerConnection(
                            incomingMessage,
                            clientConnection
                    )) {

                        log.warn(() ->
                                "[TCPProtocolProcessor][processIncoming] "
                                        + "Response received from an unregistered peer"
                                        + " | processName="
                                        + incomingMessage.getProcessName()
                                        + " | correlationId="
                                        + incomingMessage.getCorrelationId()
                        );

                        return;
                    }

                    deliverCorrelatedResponse(incomingMessage);
                }

                // ---------------------------------------------
                // HEARTBEAT
                // ---------------------------------------------

                case HEARTBEAT -> {

                    if (!handshakeManager.isRegisteredPeerConnection(
                            incomingMessage,
                            clientConnection
                    )) {

                        log.debug(
                                "[TCPProtocolProcessor][processIncoming] "
                                        + "Heartbeat ignored because peer is not registered"
                        );

                        return;
                    }

                    requireTCPMessageHandler()
                            .onHeartbeat(incomingMessage);
                }

                // ---------------------------------------------
                // INFORMATION
                // ---------------------------------------------

                case INFO -> {

                    if (!handshakeManager.isRegisteredPeerConnection(
                            incomingMessage,
                            clientConnection
                    )) {

                        log.debug(
                                "[TCPProtocolProcessor][processIncoming] "
                                        + "Information message ignored because peer is not registered"
                        );

                        return;
                    }

                    requireTCPMessageHandler()
                            .onInfo(incomingMessage);
                }

                // ---------------------------------------------
                // UNSUPPORTED MESSAGE TYPES
                // ---------------------------------------------

                default -> {

                    log.warn(() ->
                            "[TCPProtocolProcessor][processIncoming] "
                                    + "Unsupported control message type"
                                    + " | messageType=" + messageType
                                    + " | correlationId="
                                    + incomingMessage.getCorrelationId()
                    );

                    sendProtocolError(
                            incomingMessage,
                            clientConnection,
                            "UNKNOWN_CONTROL",
                            "Unsupported control message type."
                    );
                }
            }

        } catch (Exception exception) {

            log.error(
                    "[TCPProtocolProcessor][processIncoming] "
                            + "Control message processing failed",
                    exception
            );
        }
    }

    // ---------------------------------------------------------
    // INCOMING CORRELATED RESPONSES
    // ---------------------------------------------------------

    private void deliverCorrelatedResponse(
            Message response
    ) {

        UUID correlationId = Objects.requireNonNull(
                response.getCorrelationId(),
                "Control response correlation ID cannot be null"
        );

        ReplyChannel replyChannel;

        if (response.isResponseComplete()) {

            replyChannel =
                    tcpCorrelationRegistry.removeAndGet(
                            correlationId
                    );

        } else {

            replyChannel =
                    tcpCorrelationRegistry.get(
                            correlationId
                    );

            tcpCorrelationRegistry.resetTimeout(
                    correlationId
            );
        }

        if (replyChannel == null) {

            log.warn(() ->
                    "[TCPProtocolProcessor][deliverCorrelatedResponse] "
                            + "No pending control request for response"
                            + " | correlationId=" + correlationId
                            + " | responseComplete="
                            + response.isResponseComplete()
            );

            requireTCPMessageHandler().onResponse(response);
            return;
        }

        try {

            replyChannel.sendReply(response)
                    .whenComplete((ignored, failure) -> {

                        if (failure != null) {

                            log.error(
                                    "[TCPProtocolProcessor][deliverCorrelatedResponse] "
                                            + "Control response delivery failed"
                                            + " | correlationId="
                                            + correlationId,
                                    failure
                            );
                        }
                    });

        } catch (RuntimeException exception) {

            log.error(
                    "[TCPProtocolProcessor][deliverCorrelatedResponse] "
                            + "Unable to deliver control response"
                            + " | correlationId=" + correlationId,
                    exception
            );
        }
    }

    // ---------------------------------------------------------
    // INCOMING REQUEST HANDLING
    // ---------------------------------------------------------

    private void handleIncomingRequest(
            Message incomingMessage,
            ClientConnection clientConnection
    ) {

        UUID correlationId =
                incomingMessage.getCorrelationId();

        if (correlationId == null) {

            log.warn(
                    "[TCPProtocolProcessor][handleIncomingRequest] "
                            + "Received request without correlation ID"
            );

            return;
        }

        log.debug(() ->
                "[TCPProtocolProcessor][handleIncomingRequest] "
                        + "Handling registered peer request"
                        + " | correlationId=" + correlationId
                        + " | processName="
                        + incomingMessage.getProcessName()
        );

        ReplyChannel replyChannel =
                new TCPReplyChannel(
                        this,
                        clientConnection
                );

        try {

            tcpCorrelationRegistry.add(
                    correlationId,
                    replyChannel
            );

            log.debug(() ->
                    "[TCPProtocolProcessor][handleIncomingRequest] "
                            + "Control correlation registered"
                            + " | correlationId=" + correlationId
                            + " | registrySize="
                            + tcpCorrelationRegistry.getSize()
            );

            requireTCPMessageHandler()
                    .onRequest(incomingMessage);

        } catch (Exception exception) {

            tcpCorrelationRegistry.removeAndGet(
                    correlationId
            );

            log.error(
                    "[TCPProtocolProcessor][handleIncomingRequest] "
                            + "Request handler failed"
                            + " | correlationId=" + correlationId,
                    exception
            );

            sendProtocolError(
                    incomingMessage,
                    clientConnection,
                    "REQUEST_PROCESSING_FAILED",
                    exception.getMessage()
            );
        }
    }

    // ---------------------------------------------------------
    // PROTOCOL ERROR RESPONSES
    // ---------------------------------------------------------

    private void sendProtocolError(
            Message request,
            ClientConnection clientConnection,
            String errorCode,
            String errorReason
    ) {

        UUID correlationId =
                request.getCorrelationId();

        if (correlationId == null) {

            log.warn(
                    "[TCPProtocolProcessor][sendProtocolError] "
                            + "Cannot send protocol error without correlation ID"
            );

            return;
        }

        log.debug(() ->
                "[TCPProtocolProcessor][sendProtocolError] "
                        + "Creating protocol error response"
                        + " | correlationId=" + correlationId
                        + " | errorCode=" + errorCode
        );

        Message errorResponse =
                messageParser.createErrorResponse(
                        correlationId,
                        errorCode,
                        errorReason == null
                                ? "Unknown processing error."
                                : errorReason
                );

        processOutgoing(
                errorResponse,
                clientConnection
        ).whenComplete((ignored, exception) -> {

            if (exception != null) {

                log.error(
                        "[TCPProtocolProcessor][sendProtocolError] "
                                + "Unable to send protocol error"
                                + " | correlationId=" + correlationId
                                + " | errorCode=" + errorCode,
                        exception
                );
            }
        });
    }

    // ---------------------------------------------------------
    // SERIALIZATION
    // ---------------------------------------------------------

    @Override
    public String serialize(
            Message message
    ) {

        String serializedMessage =
                messageParser.createJSONFromMessage(
                        Objects.requireNonNull(message)
                );

        log.debug(() ->
                "[TCPProtocolProcessor][serialize] "
                        + "Control message serialized"
                        + " | messageType="
                        + message.getMessageType()
                        + " | operation=" + message.getOperation()
                        + " | serializedLength="
                        + (serializedMessage == null
                        ? 0
                        : serializedMessage.length())
        );

        return serializedMessage;
    }

    // ---------------------------------------------------------
    // DIRECT CONNECTION WRITE
    // ---------------------------------------------------------

    /*
     * Used for handshake replies and responses that must
     * be sent on the exact connection that received a request.
     *
     * This deliberately bypasses peer lookup because the
     * connection may not yet be registered.
     */
    @Override
    public CompletionStage<Void> processOutgoing(
            Message outgoing,
            ClientConnection clientConnection
    ) {

        Objects.requireNonNull(outgoing);
        Objects.requireNonNull(clientConnection);

        log.debug(() ->
                "[TCPProtocolProcessor][processOutgoing] "
                        + "Sending message directly over supplied connection"
                        + " | messageType="
                        + outgoing.getMessageType()
                        + " | operation=" + outgoing.getOperation()
                        + " | correlationId="
                        + outgoing.getCorrelationId()
        );

        try {

            clientConnection.sendMessage(
                    serialize(outgoing)
            );

            log.debug(() ->
                    "[TCPProtocolProcessor][processOutgoing] "
                            + "Direct socket write completed"
                            + " | messageType="
                            + outgoing.getMessageType()
                            + " | correlationId="
                            + outgoing.getCorrelationId()
            );

            return CompletableFuture.completedFuture(null);

        } catch (Exception exception) {

            log.error(
                    "[TCPProtocolProcessor][processOutgoing] "
                            + "Direct socket write failed"
                            + " | messageType="
                            + outgoing.getMessageType()
                            + " | correlationId="
                            + outgoing.getCorrelationId(),
                    exception
            );

            return CompletableFuture.failedFuture(exception);
        }
    }

    // ---------------------------------------------------------
    // PEER RECONCILIATION
    // ---------------------------------------------------------

    @Override
    public synchronized void reconcilePeers() {

        log.debug(() ->
                "[TCPProtocolProcessor][reconcilePeers] "
                        + "Starting peer reconciliation"
                        + " | connectionMode=" + connectionMode
                        + " | configuredPeers="
                        + configuredPeers.size()
                        + " | activePeers="
                        + handshakeManager.getActivePeerCount()
                        + " | pendingHandshakes="
                        + handshakeManager.getPendingHandshakeCount()
        );

        if (connectionMode == TCPConnectionMode.SERVER_ONLY) {

            log.debug(
                    "[TCPProtocolProcessor][reconcilePeers] "
                            + "Peer reconciliation skipped because connection mode is SERVER_ONLY"
            );

            return;
        }

        TCPService service = tcpService;

        if (service == null) {

            log.error(
                    "[TCPProtocolProcessor][reconcilePeers] "
                            + "TCP service has not been initialized"
            );

            return;
        }

        /*
         * Remove pending handshakes that have
         * exceeded their timeout.
         */
        handshakeManager.cleanupExpiredHandshakes();

        for (TCPPeer peer : configuredPeers) {

            String peerId =
                    TCPHandshakeManager.normalizePeerId(
                            peer.processId()
                    );

            if (peerId == null) {
                continue;
            }

            log.debug(() ->
                    "[TCPProtocolProcessor][reconcilePeers] "
                            + "Evaluating configured peer"
                            + " | peerId=" + peerId
                            + " | host=" + peer.host()
                            + " | port=" + peer.port()
            );

            // ---------------------------------------------
            // Check existing active connection
            // ---------------------------------------------

            ClientConnection active =
                    handshakeManager.getActiveConnection(
                            peerId
                    );

            if (active != null) {

                if (TCPHandshakeManager.isConnectionUsable(active)) {

                    log.debug(() ->
                            "[TCPProtocolProcessor][reconcilePeers] "
                                    + "Peer already has a usable active connection"
                                    + " | peerId=" + peerId
                    );

                    continue;
                }

                handshakeManager.removePeerConnection(
                        peerId,
                        active
                );

                log.warn(() ->
                        "[TCPProtocolProcessor][reconcilePeers] "
                                + "Removed unusable active peer connection"
                                + " | peerId=" + peerId
                );
            }

            // ---------------------------------------------
            // Check pending handshake
            // ---------------------------------------------

            if (handshakeManager.hasPendingHandshake(peerId)) {

                log.debug(() ->
                        "[TCPProtocolProcessor][reconcilePeers] "
                                + "Connection attempt skipped because handshake is pending"
                                + " | peerId=" + peerId
                );

                continue;
            }

            // ---------------------------------------------
            // Establish TCP connection
            // ---------------------------------------------

            ClientConnection connection;

            try {

                log.debug(() ->
                        "[TCPProtocolProcessor][reconcilePeers] "
                                + "Attempting TCP connection to peer"
                                + " | peerId=" + peerId
                                + " | host=" + peer.host()
                                + " | port=" + peer.port()
                );

                connection =
                        service.attemptConnection(peer);

            } catch (Exception exception) {

                log.error(
                        "[TCPProtocolProcessor][reconcilePeers] "
                                + "Connection attempt failed"
                                + " | peerId=" + peerId,
                        exception
                );

                continue;
            }

            if (connection == null) {

                log.debug(() ->
                        "[TCPProtocolProcessor][reconcilePeers] "
                                + "TCP service returned no connection"
                                + " | peerId=" + peerId
                );

                continue;
            }

            // ---------------------------------------------
            // Initiate handshake
            // ---------------------------------------------

            /*
             * The manager now owns the pending registration,
             * handshake creation, delivery and cleanup.
             *
             * A successful write does not mean the
             * remote peer has accepted the handshake.
             */
            handshakeManager.initiateHandshake(
                    peerId,
                    connection
            ).whenComplete((ignored, exception) -> {

                if (exception != null) {

                    log.error(
                            "[TCPProtocolProcessor][reconcilePeers] "
                                    + "Handshake request failed"
                                    + " | peerId=" + peerId,
                            exception
                    );
                }
            });
        }

        log.debug(() ->
                "[TCPProtocolProcessor][reconcilePeers] "
                        + "Peer reconciliation completed"
                        + " | activePeers="
                        + handshakeManager.getActivePeerCount()
                        + " | pendingHandshakes="
                        + handshakeManager.getPendingHandshakeCount()
        );
    }

    // ---------------------------------------------------------
    // OUTGOING MESSAGE ROUTING
    // ---------------------------------------------------------

    public CompletionStage<Void> processOutgoing(
            Message outgoing
    ) {

        Objects.requireNonNull(outgoing);

        log.debug(() ->
                "[TCPProtocolProcessor][processOutgoing] "
                        + "Routing outgoing control message"
                        + " | messageType="
                        + outgoing.getMessageType()
                        + " | operation="
                        + outgoing.getOperation()
                        + " | correlationId="
                        + outgoing.getCorrelationId()
                        + " | recipientId="
                        + outgoing.getRecipientID()
        );

        // ---------------------------------------------
        // RESPONSE: use correlated reply channel
        // ---------------------------------------------

        if (outgoing.getMessageType() == MessageType.RESPONSE) {

            UUID correlationId =
                    outgoing.getCorrelationId();

            if (correlationId == null) {

                log.warn(
                        "[TCPProtocolProcessor][processOutgoing] "
                                + "Response rejected because correlation ID is null"
                );

                return CompletableFuture.failedFuture(
                        new IllegalArgumentException(
                                "Response correlation ID cannot be null"
                        )
                );
            }

            ReplyChannel replyChannel;

            if (outgoing.isResponseComplete()) {

                log.debug(() ->
                        "[TCPProtocolProcessor][processOutgoing] "
                                + "Final response; removing control correlation"
                                + " | correlationId=" + correlationId
                );

                replyChannel =
                        tcpCorrelationRegistry.removeAndGet(
                                correlationId
                        );

            } else {

                log.debug(() ->
                        "[TCPProtocolProcessor][processOutgoing] "
                                + "Intermediate response; retaining control correlation"
                                + " | correlationId=" + correlationId
                );

                replyChannel =
                        tcpCorrelationRegistry.get(
                                correlationId
                        );

                tcpCorrelationRegistry.resetTimeout(
                        correlationId
                );
            }

            if (replyChannel == null) {

                log.warn(() ->
                        "[TCPProtocolProcessor][processOutgoing] "
                                + "No pending reply channel found"
                                + " | correlationId=" + correlationId
                );

                return CompletableFuture.failedFuture(
                        new IllegalStateException(
                                "No pending reply channel for correlation ID: "
                                        + correlationId
                        )
                );
            }

            log.debug(() ->
                    "[TCPProtocolProcessor][processOutgoing] "
                            + "Dispatching response through correlated reply channel"
                            + " | correlationId=" + correlationId
            );

            return replyChannel.sendReply(outgoing);
        }

        // ---------------------------------------------
        // Other message types: route by recipient ID
        // ---------------------------------------------

        String recipientId =
                TCPHandshakeManager.normalizePeerId(
                        outgoing.getRecipientID()
                );

        if (recipientId == null) {

            log.warn(
                    "[TCPProtocolProcessor][processOutgoing] "
                            + "Outgoing message rejected because recipient ID is missing"
            );

            return CompletableFuture.failedFuture(
                    new IllegalArgumentException(
                            "Outgoing message recipient ID is missing"
                    )
            );
        }

        ClientConnection connection =
                handshakeManager.getActiveConnection(
                        recipientId
                );

        if (connection == null) {

            log.warn(() ->
                    "[TCPProtocolProcessor][processOutgoing] "
                            + "No active connection found for recipient"
                            + " | recipientId=" + recipientId
            );

            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "No active connection for peer: "
                                    + recipientId
                    )
            );
        }

        if (!TCPHandshakeManager.isConnectionUsable(connection)) {

            handshakeManager.removePeerConnection(
                    recipientId,
                    connection
            );

            try {
                connection.getSocket().close();
            } catch (Exception exception) {

                log.error(
                        "[TCPProtocolProcessor][processOutgoing] "
                                + "Unable to close unusable connection"
                                + " | recipientId=" + recipientId,
                        exception
                );
            }

            log.warn(() ->
                    "[TCPProtocolProcessor][processOutgoing] "
                            + "Recipient connection is no longer usable"
                            + " | recipientId=" + recipientId
            );

            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "Connection is no longer usable for peer: "
                                    + recipientId
                    )
            );
        }

        TCPService service = tcpService;

        if (service == null) {

            log.error(
                    "[TCPProtocolProcessor][processOutgoing] "
                            + "TCP service has not been initialized"
            );

            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "TCP service has not been initialized"
                    )
            );
        }

        log.debug(() ->
                "[TCPProtocolProcessor][processOutgoing] "
                        + "Writing message to active peer connection"
                        + " | recipientId=" + recipientId
                        + " | messageType="
                        + outgoing.getMessageType()
        );

        return service.writeMessage(
                serialize(outgoing),
                connection
        );
    }

    // ---------------------------------------------------------
    // REPLY CHANNEL OUTGOING MESSAGE
    // ---------------------------------------------------------

    public CompletionStage<Void> processOutgoing(
            Message outgoing,
            ReplyChannel replyChannel
    ) {

        Objects.requireNonNull(outgoing);
        Objects.requireNonNull(replyChannel);

        log.debug(() ->
                "[TCPProtocolProcessor][processOutgoing] "
                        + "Sending message through supplied reply channel"
                        + " | messageType="
                        + outgoing.getMessageType()
                        + " | correlationId="
                        + outgoing.getCorrelationId()
                        + " | replyChannel="
                        + replyChannel.getClass().getSimpleName()
        );

        return replyChannel.sendReply(outgoing);
    }

    // ---------------------------------------------------------
    // CORRELATION CLEANUP
    // ---------------------------------------------------------

    public void cleanupExpiredCorrelations() {

        log.debug(() ->
                "[TCPProtocolProcessor][cleanupExpiredCorrelations] "
                        + "Starting control correlation cleanup"
                        + " | currentCorrelations="
                        + tcpCorrelationRegistry.getSize()
                        + " | timeoutMillis="
                        + CORRELATION_TIMEOUT_MILLIS
        );

        int expiredCount = 0;

        for (TCPCorrelationRegistry.ExpiredCorrelation expired :
                tcpCorrelationRegistry.removeExpired(
                        CORRELATION_TIMEOUT_MILLIS
                )) {

            expiredCount++;

            log.warn(() ->
                    "[TCPProtocolProcessor][cleanupExpiredCorrelations] "
                            + "Control request correlation expired"
                            + " | correlationId="
                            + expired.correlationId()
            );

            try {

                Message failure =
                        messageParser.createErrorResponse(
                                expired.correlationId(),
                                "REQUEST_TIMEOUT",
                                "Control request timed out."
                        );

                expired.replyChannel()
                        .sendReply(failure)
                        .whenComplete((ignored, exception) -> {

                            if (exception != null) {

                                log.error(
                                        "[TCPProtocolProcessor][cleanupExpiredCorrelations] "
                                                + "Timeout reply delivery failed"
                                                + " | correlationId="
                                                + expired.correlationId(),
                                        exception
                                );
                            }
                        });

            } catch (Exception exception) {

                log.error(
                        "[TCPProtocolProcessor][cleanupExpiredCorrelations] "
                                + "Timeout cleanup failed"
                                + " | correlationId="
                                + expired.correlationId(),
                        exception
                );
            }
        }

        final int expiredCorrelationCount =
                expiredCount;

        log.debug(() ->
                "[TCPProtocolProcessor][cleanupExpiredCorrelations] "
                        + "Control correlation cleanup completed"
                        + " | expiredCorrelations="
                        + expiredCorrelationCount
                        + " | remainingCorrelations="
                        + tcpCorrelationRegistry.getSize()
        );
    }

    // ---------------------------------------------------------
    // CONTROL CHANNEL COMMUNICATOR
    // ---------------------------------------------------------

    @Override
    public CompletionStage<Void> sendRequest(
            Message request
    ) {

        Objects.requireNonNull(request);

        log.debug(() ->
                "[TCPProtocolProcessor][sendRequest] "
                        + "Sending control request"
                        + " | correlationId="
                        + request.getCorrelationId()
                        + " | recipientId="
                        + request.getRecipientID()
        );

        return processOutgoing(request);
    }

    public CompletionStage<Void> sendRequest(
            Message request,
            ReplyChannel replyChannel
    ) {

        Objects.requireNonNull(request);
        Objects.requireNonNull(replyChannel);

        UUID correlationId =
                Objects.requireNonNull(
                        request.getCorrelationId(),
                        "Request correlation ID cannot be null"
                );

        log.debug(() ->
                "[TCPProtocolProcessor][sendRequest] "
                        + "Registering reply channel for outgoing control request"
                        + " | correlationId=" + correlationId
        );

        tcpCorrelationRegistry.add(
                correlationId,
                replyChannel
        );

        CompletionStage<Void> writeStage;

        try {

            writeStage = processOutgoing(request);

        } catch (RuntimeException exception) {

            tcpCorrelationRegistry.removeAndGet(
                    correlationId
            );

            log.error(
                    "[TCPProtocolProcessor][sendRequest] "
                            + "Request submission failed; correlation removed"
                            + " | correlationId=" + correlationId,
                    exception
            );

            return CompletableFuture.failedFuture(exception);
        }

        return writeStage.whenComplete((ignored, exception) -> {

            if (exception != null) {

                tcpCorrelationRegistry.removeAndGet(
                        correlationId
                );

                log.error(
                        "[TCPProtocolProcessor][sendRequest] "
                                + "Control request delivery failed; correlation removed"
                                + " | correlationId=" + correlationId,
                        exception
                );

            } else {

                log.debug(() ->
                        "[TCPProtocolProcessor][sendRequest] "
                                + "Control request submitted successfully"
                                + " | correlationId=" + correlationId
                );
            }
        });
    }

    @Override
    public void sendResponse(
            Message response
    ) {

        log.debug(() ->
                "[TCPProtocolProcessor][sendResponse] "
                        + "Sending control response"
                        + " | correlationId="
                        + response.getCorrelationId()
                        + " | messageType="
                        + response.getMessageType()
        );

        processOutgoing(response)
                .whenComplete((ignored, exception) -> {

                    if (exception != null) {

                        log.error(
                                "[TCPProtocolProcessor][sendResponse] "
                                        + "Unable to send control response"
                                        + " | correlationId="
                                        + response.getCorrelationId(),
                                exception
                        );
                    }
                });
    }

    @Override
    public void disconnect(
            String connectionName
    ) {

        if (connectionName == null ||
                connectionName.isBlank()) {
            return;
        }

        log.info(() ->
                "[TCPProtocolProcessor][disconnect] "
                        + "Disconnecting peer"
                        + " | peerId=" + connectionName
        );

        handshakeManager.disconnect(connectionName);

        log.info(() ->
                "[TCPProtocolProcessor][disconnect] "
                        + "Peer disconnected"
                        + " | peerId=" + connectionName
                        + " | activePeers="
                        + handshakeManager.getActivePeerCount()
        );
    }

    // ---------------------------------------------------------
    // TCP PROCESSOR LIFECYCLE
    // ---------------------------------------------------------

    @Override
    public void cleanupCorrelations() {

        log.debug(
                "[TCPProtocolProcessor][cleanupCorrelations] "
                        + "TCP control cleanup cycle started"
        );

        cleanupExpiredCorrelations();

        handshakeManager.cleanupExpiredHandshakes();

        log.debug(
                "[TCPProtocolProcessor][cleanupCorrelations] "
                        + "TCP control cleanup cycle completed"
        );
    }

    @Override
    public void informShutdown() {

        log.info(() ->
                "[TCPProtocolProcessor][informShutdown] "
                        + "TCP protocol processor shutdown initiated"
                        + " | activePeers="
                        + handshakeManager.getActivePeerCount()
                        + " | pendingHandshakes="
                        + handshakeManager.getPendingHandshakeCount()
                        + " | correlations="
                        + tcpCorrelationRegistry.getSize()
        );

        // ---------------------------------------------
        // Fail outstanding correlated requests
        // ---------------------------------------------

        for (TCPCorrelationRegistry.ExpiredCorrelation expired :
                tcpCorrelationRegistry.removeExpired(0)) {

            try {

                Message failure =
                        messageParser.createErrorResponse(
                                expired.correlationId(),
                                "CONTROL_PROCESSOR_SHUTDOWN",
                                "Control processor is shutting down."
                        );

                expired.replyChannel()
                        .sendReply(failure)
                        .whenComplete((ignored, exception) -> {

                            if (exception != null) {

                                log.error(
                                        "[TCPProtocolProcessor][informShutdown] "
                                                + "Shutdown reply delivery failed"
                                                + " | correlationId="
                                                + expired.correlationId(),
                                        exception
                                );
                            }
                        });

            } catch (Exception exception) {

                log.error(
                        "[TCPProtocolProcessor][informShutdown] "
                                + "Shutdown notification failed"
                                + " | correlationId="
                                + expired.correlationId(),
                        exception
                );
            }
        }

        // ---------------------------------------------
        // Close active and pending control connections
        // ---------------------------------------------

        handshakeManager.shutdown();

        log.info(
                "[TCPProtocolProcessor][informShutdown] "
                        + "TCP protocol processor shutdown cleanup completed"
        );
    }

    @Override
    public void onConnectionClosed(
            ClientConnection closedConnection
    ) {

        if (closedConnection == null) {
            return;
        }

        log.debug(
                "[TCPProtocolProcessor][onConnectionClosed] "
                        + "Connection closure notification received"
        );

        /*
         * Remove all active and pending registrations
         * associated with this socket.
         */
        handshakeManager.onConnectionClosed(
                closedConnection
        );
    }
}
