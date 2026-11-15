package org.prats.processors;

import com.fasterxml.jackson.core.JsonProcessingException;

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

import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;

import org.prats.registries.TCPControlRegistry;

import org.prats.replyChannels.TCPReplyChannel;

import org.prats.services.tcpService.TCPService;

import java.io.IOException;
import java.net.Socket;

import java.util.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


public class TCPControlProcessor
        implements TCPProcessor, ControlChannelCommunicator {

    private static final EngineLogger log =
            EngineLogger.getLogger(TCPControlProcessor.class);


    // ---------------------------------------------------------
    // CONSTANTS
    // ---------------------------------------------------------

    private static final long CORRELATION_TIMEOUT_MILLIS =
            120_000L;

    private static final long HANDSHAKE_TIMEOUT_MILLIS =
            15_000L;

    private static final long HANDSHAKE_TIMEOUT_NANOS =
            TimeUnit.MILLISECONDS.toNanos(
                    HANDSHAKE_TIMEOUT_MILLIS
            );

    private static final String HANDSHAKE_ACCEPTED =
            "Accepted";

    private static final String HANDSHAKE_REJECTED =
            "Rejected";


    // ---------------------------------------------------------
    // PROCESSOR CONFIGURATION
    // ---------------------------------------------------------

    private final TCPConnectionMode connectionMode;

    private final MessageParser messageParser;

    private final List<TCPPeer> configuredPeers;


    // ---------------------------------------------------------
    // REQUEST / RESPONSE CORRELATION
    // ---------------------------------------------------------

    private final TCPControlRegistry tcpControlRegistry =
            new TCPControlRegistry();


    // ---------------------------------------------------------
    // PEER CONNECTION REGISTRIES
    // ---------------------------------------------------------

    /*
     * Connections that have successfully completed
     * the handshake.
     */
    private final Map<String, ClientConnection> activePeerConnections =
            new ConcurrentHashMap<>();


    /*
     * Connections that have been opened but have not
     * completed their handshake.
     */
    private final Map<String, PendingHandshake> pendingPeerConnections =
            new ConcurrentHashMap<>();


    /*
     * Tracks when an outgoing handshake was initiated.
     */
    private record PendingHandshake(
            ClientConnection connection,
            long initiatedAtNanos
    ) {}


    // ---------------------------------------------------------
    // DEPENDENCIES
    // ---------------------------------------------------------

    private volatile TCPService tcpService;

    @Override
    public java.util.List<java.net.Socket> connectedSockets() {
        return activePeerConnections.values().stream().map(ClientConnection::getSocket)
                .filter(socket -> !socket.isClosed()).toList();
    }

    public boolean isPeerConnected(String peerId) {
        ClientConnection connection = activePeerConnections.get(peerId == null ? "" : peerId.trim().toUpperCase(Locale.ROOT));
        return connection != null && !connection.getSocket().isClosed();
    }

    private volatile ControlMessageHandler controlMessageHandler;


    // ---------------------------------------------------------
    // CONSTRUCTOR
    // ---------------------------------------------------------

    public TCPControlProcessor(
            TCPConnectionMode tcpConnectionMode,
            List<TCPPeer> configuredPeers,
            MessageParser messageParser
    ) {

        this.connectionMode =
                Objects.requireNonNull(
                        tcpConnectionMode,
                        "TCP connection mode cannot be null"
                );

        this.messageParser =
                Objects.requireNonNull(
                        messageParser,
                        "Message parser cannot be null"
                );

        this.configuredPeers =
                List.copyOf(
                        Objects.requireNonNull(
                                configuredPeers,
                                "Configured peer list cannot be null"
                        )
                );

        log.info(() ->
                "[TCPControlProcessor][constructor] TCP control processor initialized"
                        + " | connectionMode=" + connectionMode
                        + " | configuredPeers=" + this.configuredPeers.size()
        );
    }


    // ---------------------------------------------------------
    // DEPENDENCY INJECTION
    // ---------------------------------------------------------

    public void setTcpService(TCPService tcpService) {

        this.tcpService = Objects.requireNonNull(
                tcpService,
                "TCP service cannot be null"
        );

        log.debug(() ->
                "[TCPControlProcessor][setTcpService] TCP service configured"
                        + " | serviceType=" + tcpService.getClass().getSimpleName()
        );
    }


    public void setControlMessageHandler(
            ControlMessageHandler controlMessageHandler
    ) {

        this.controlMessageHandler = Objects.requireNonNull(
                controlMessageHandler,
                "Control message handler cannot be null"
        );

        log.debug(() ->
                "[TCPControlProcessor][setControlMessageHandler] Control message handler configured"
                        + " | handlerType="
                        + controlMessageHandler.getClass().getSimpleName()
        );
    }


    // ---------------------------------------------------------
    // PEER CONNECTION MANAGEMENT
    // ---------------------------------------------------------

    public void registerPeerConnection(
            String peerId,
            ClientConnection connection
    ) {

        Objects.requireNonNull(peerId);
        Objects.requireNonNull(connection);

        if (peerId.isBlank()) {
            log.warn(
                    "[TCPControlProcessor][registerPeerConnection] Peer registration rejected because peer ID is blank"
            );

            throw new IllegalArgumentException(
                    "Peer ID cannot be blank"
            );
        }

        log.debug(() ->
                "[TCPControlProcessor][registerPeerConnection] Registering peer connection"
                        + " | peerId=" + peerId
        );

        if (!registerActivePeer(peerId, connection)) {

            log.warn(() ->
                    "[TCPControlProcessor][registerPeerConnection] A different active connection already exists"
                            + " | peerId=" + peerId
            );

            throw new IllegalStateException(
                    "A different active connection already exists for peer: "
                            + peerId
            );
        }

        log.info(() ->
                "[TCPControlProcessor][registerPeerConnection] Peer connection registered"
                        + " | peerId=" + peerId
                        + " | activePeerConnections=" + activePeerConnections.size()
        );
    }


    private boolean registerActivePeer(
            String peerId,
            ClientConnection connection
    ) {

        Objects.requireNonNull(peerId);
        Objects.requireNonNull(connection);

        peerId = normalizePeerId(peerId);

        if (peerId == null) {
            log.debug(()->
                    "[TCPControlProcessor][registerActivePeer] Peer registration skipped because normalized peer ID is null"
            );

            return false;
        }

        final String normalizedPeerId = peerId;

        log.debug(() ->
                "[TCPControlProcessor][registerActivePeer] Attempting active peer registration"
                        + " | peerId=" + normalizedPeerId
        );

        while (true) {

            ClientConnection existing =
                    activePeerConnections.putIfAbsent(
                            peerId,
                            connection
                    );

            if (existing == null || existing == connection) {

                log.debug(() ->
                        "[TCPControlProcessor][registerActivePeer] Active peer connection registered"
                                + " | peerId=" + normalizedPeerId
                                + " | alreadyRegistered=" + (existing == connection)
                );

                return true;
            }

            /*
             * Replace an existing connection only when
             * its socket is no longer usable.
             */
            if (!isConnectionUsable(existing)) {

                if (activePeerConnections.replace(
                        peerId,
                        existing,
                        connection
                )) {

                    log.info(() ->
                            "[TCPControlProcessor][registerActivePeer] Replaced unusable peer connection"
                                    + " | peerId=" + normalizedPeerId
                    );

                    return true;
                }

                continue;
            }

            log.warn(() ->
                    "[TCPControlProcessor][registerActivePeer] Another usable connection already exists"
                            + " | peerId=" + normalizedPeerId
            );

            return false;
        }
    }


    public void removePeerConnection(
            String peerId,
            ClientConnection connection
    ) {

        if (peerId == null || connection == null) {
            return;
        }

        final String normalizedPeerId = normalizePeerId(peerId);

        activePeerConnections.remove(
                normalizedPeerId,
                connection
        );

        PendingHandshake pending =
                pendingPeerConnections.get(normalizedPeerId);

        if (pending != null &&
                pending.connection() == connection) {

            pendingPeerConnections.remove(
                    normalizedPeerId,
                    pending
            );
        }

        log.debug(() ->
                "[TCPControlProcessor][removePeerConnection] Peer connection removal completed"
                        + " | peerId=" + normalizedPeerId
                        + " | activePeerConnections=" + activePeerConnections.size()
                        + " | pendingPeerConnections=" + pendingPeerConnections.size()
        );
    }


    private boolean isConnectionUsable(
            ClientConnection connection
    ) {

        if (connection == null) {
            return false;
        }

        try {

            Socket socket = connection.getSocket();

            return socket != null
                    && socket.isConnected()
                    && !socket.isClosed()
                    && !socket.isInputShutdown()
                    && !socket.isOutputShutdown();

        } catch (RuntimeException exception) {

            return false;
        }
    }


    private boolean isConfiguredPeer(String peerId) {

        String normalizedPeerId = normalizePeerId(peerId);

        if (normalizedPeerId == null) {
            return false;
        }

        boolean configured = configuredPeers.stream()
                .map(TCPPeer::processId)
                .map(TCPControlProcessor::normalizePeerId)
                .anyMatch(normalizedPeerId::equals);

        log.debug(() ->
                "[TCPControlProcessor][isConfiguredPeer] Peer configuration lookup completed"
                        + " | peerId=" + normalizedPeerId
                        + " | configured=" + configured
        );

        return configured;
    }


    private boolean isRegisteredPeerConnection(
            Message message,
            ClientConnection connection
    ) {

        String senderId = normalizePeerId(message.getProcessName());

        if (senderId == null || senderId.isBlank()) {
            return false;
        }

        ClientConnection registeredConnection =
                activePeerConnections.get(senderId);

        boolean registered = registeredConnection == connection
                && isConnectionUsable(connection);

        log.debug(() ->
                "[TCPControlProcessor][isRegisteredPeerConnection] Peer connection authentication evaluated"
                        + " | peerId=" + senderId
                        + " | registered=" + registered
        );

        return registered;
    }


    private void closeConnection(
            ClientConnection connection
    ) {

        if (connection == null) {
            return;
        }

        try {

            connection.getSocket().close();

            log.debug(
                    "[TCPControlProcessor][closeConnection] Socket connection closed"
            );

        } catch (IOException exception) {

            log.error(
                    "[TCPControlProcessor][closeConnection] Unable to close connection",
                    exception
            );
        }
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
                    "[TCPControlProcessor][processIncoming] Empty control message ignored"
            );

            return;
        }

        Objects.requireNonNull(
                clientConnection,
                "Client connection cannot be null"
        );

        log.debug(() ->
                "[TCPControlProcessor][processIncoming] Incoming control message received"
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
                    "[TCPControlProcessor][processIncoming] Message parsing failed",
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
                    "[TCPControlProcessor][processIncoming] Control message parsed"
                            + " | messageType=" + messageType
                            + " | operation=" + incomingMessage.getOperation()
                            + " | processName=" + incomingMessage.getProcessName()
                            + " | correlationId=" + incomingMessage.getCorrelationId()
            );


            switch (messageType) {

                // ---------------------------------------------
                // HANDSHAKE
                // ---------------------------------------------

                case HANDSHAKE_REQUEST -> {

                    log.debug(
                            "[TCPControlProcessor][processIncoming] Handshake request received"
                    );

                    resolveHandshakeRequest(
                            incomingMessage,
                            clientConnection
                    );
                }


                case HANDSHAKE_RESPONSE -> {

                    log.debug(
                            "[TCPControlProcessor][processIncoming] Handshake response received"
                    );

                    resolveHandshakeResponse(
                            incomingMessage,
                            clientConnection
                    );
                }


                // ---------------------------------------------
                // REQUEST
                // ---------------------------------------------

                case REQUEST -> {

                    if (!isRegisteredPeerConnection(
                            incomingMessage,
                            clientConnection
                    )) {

                        log.warn(() ->
                                "[TCPControlProcessor][processIncoming] Request rejected because peer has not completed handshake"
                                        + " | processName=" + incomingMessage.getProcessName()
                                        + " | correlationId=" + incomingMessage.getCorrelationId()
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

                    if (!isRegisteredPeerConnection(
                            incomingMessage,
                            clientConnection
                    )) {

                        log.warn(() ->
                                "[TCPControlProcessor][processIncoming] Response received from an unregistered peer"
                                        + " | processName=" + incomingMessage.getProcessName()
                                        + " | correlationId=" + incomingMessage.getCorrelationId()
                        );

                        return;
                    }

                    deliverCorrelatedResponse(incomingMessage);
                }


                // ---------------------------------------------
                // HEARTBEAT
                // ---------------------------------------------

                case HEARTBEAT -> {

                    if (!isRegisteredPeerConnection(
                            incomingMessage,
                            clientConnection
                    )) {

                        log.debug(
                                "[TCPControlProcessor][processIncoming] Heartbeat ignored because peer is not registered"
                        );

                        return;
                    }

                    requireControlMessageHandler()
                            .onHeartbeat(incomingMessage);
                }


                // ---------------------------------------------
                // INFORMATION
                // ---------------------------------------------

                case INFO -> {

                    if (!isRegisteredPeerConnection(
                            incomingMessage,
                            clientConnection
                    )) {

                        log.debug(
                                "[TCPControlProcessor][processIncoming] Information message ignored because peer is not registered"
                        );

                        return;
                    }

                    requireControlMessageHandler()
                            .onInfo(incomingMessage);
                }


                // ---------------------------------------------
                // UNKNOWN MESSAGE TYPE
                // ---------------------------------------------

                default -> {

                    log.warn(() ->
                            "[TCPControlProcessor][processIncoming] Unsupported control message type"
                                    + " | messageType=" + messageType
                                    + " | correlationId=" + incomingMessage.getCorrelationId()
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
                    "[TCPControlProcessor][processIncoming] Control message processing failed",
                    exception
            );
        }
    }


    private ControlMessageHandler requireControlMessageHandler() {

        return Objects.requireNonNull(
                controlMessageHandler,
                "Control message handler has not been initialized"
        );
    }

    private void deliverCorrelatedResponse(Message response) {
        UUID correlationId = Objects.requireNonNull(
                response.getCorrelationId(),
                "Control response correlation ID cannot be null"
        );

        ReplyChannel replyChannel;
        if (response.isResponseComplete()) {
            replyChannel = tcpControlRegistry.removeAndGet(correlationId);
        } else {
            replyChannel = tcpControlRegistry.get(correlationId);
            tcpControlRegistry.resetTimeout(correlationId);
        }

        if (replyChannel == null) {
            log.warn(() ->
                    "[TCPControlProcessor][deliverCorrelatedResponse] No pending control request for response"
                            + " | correlationId=" + correlationId
                            + " | responseComplete=" + response.isResponseComplete()
            );
            requireControlMessageHandler().onResponse(response);
            return;
        }

        try {
            replyChannel.sendReply(response).whenComplete((ignored, failure) -> {
                if (failure != null) {
                    log.error(
                            "[TCPControlProcessor][deliverCorrelatedResponse] Control response delivery failed"
                                    + " | correlationId=" + correlationId,
                            failure
                    );
                }
            });
        } catch (RuntimeException failure) {
            log.error(
                    "[TCPControlProcessor][deliverCorrelatedResponse] Unable to deliver control response"
                            + " | correlationId=" + correlationId,
                    failure
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
                    "[TCPControlProcessor][handleIncomingRequest] Received request without correlation ID"
            );

            return;
        }

        log.debug(() ->
                "[TCPControlProcessor][handleIncomingRequest] Handling authenticated peer request"
                        + " | correlationId=" + correlationId
                        + " | processName=" + incomingMessage.getProcessName()
        );

        ReplyChannel replyChannel =
                new TCPReplyChannel(
                        this,
                        clientConnection
                );


        try {

            tcpControlRegistry.add(
                    correlationId,
                    replyChannel
            );

            log.debug(() ->
                    "[TCPControlProcessor][handleIncomingRequest] Control correlation registered"
                            + " | correlationId=" + correlationId
                            + " | registrySize=" + tcpControlRegistry.getSize()
            );

            requireControlMessageHandler()
                    .onRequest(incomingMessage);

        } catch (Exception exception) {

            tcpControlRegistry.removeAndGet(correlationId);

            log.error(
                    "[TCPControlProcessor][handleIncomingRequest] Request handler failed"
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


    private void sendProtocolError(
            Message request,
            ClientConnection clientConnection,
            String errorCode,
            String errorReason
    ) {

        UUID correlationId = request.getCorrelationId();

        if (correlationId == null) {
            return;
        }

        log.debug(() ->
                "[TCPControlProcessor][sendProtocolError] Creating protocol error response"
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
                        "[TCPControlProcessor][sendProtocolError] Unable to send protocol error"
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
    public String serialize(Message message) {

        String serializedMessage = messageParser.createJSONFromMessage(
                Objects.requireNonNull(message)
        );

        log.debug(() ->
                "[TCPControlProcessor][serialize] Control message serialized"
                        + " | messageType=" + message.getMessageType()
                        + " | operation=" + message.getOperation()
                        + " | serializedLength="
                        + (serializedMessage == null ? 0 : serializedMessage.length())
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
     * connection may not yet be present in activePeerConnections.
     */

    @Override
    public CompletionStage<Void> processOutgoing(
            Message outgoing,
            ClientConnection clientConnection
    ) {

        Objects.requireNonNull(outgoing);
        Objects.requireNonNull(clientConnection);

        log.debug(() ->
                "[TCPControlProcessor][processOutgoing] Sending message directly over supplied connection"
                        + " | messageType=" + outgoing.getMessageType()
                        + " | operation=" + outgoing.getOperation()
                        + " | correlationId=" + outgoing.getCorrelationId()
        );

        try {

            clientConnection.sendMessage(
                    serialize(outgoing)
            );

            log.debug(() ->
                    "[TCPControlProcessor][processOutgoing] Direct socket write completed"
                            + " | messageType=" + outgoing.getMessageType()
                            + " | correlationId=" + outgoing.getCorrelationId()
            );

            return CompletableFuture.completedFuture(null);

        } catch (Exception exception) {

            log.error(
                    "[TCPControlProcessor][processOutgoing] Direct socket write failed"
                            + " | messageType=" + outgoing.getMessageType()
                            + " | correlationId=" + outgoing.getCorrelationId(),
                    exception
            );

            return CompletableFuture.failedFuture(exception);
        }
    }


    // ---------------------------------------------------------
    // HANDSHAKE MESSAGE CREATION
    // ---------------------------------------------------------

    private Message createHandshakeMessage(
            MessageType handshakeType,
            String body
    ) {

        log.debug(() ->
                "[TCPControlProcessor][createHandshakeMessage] Creating handshake message"
                        + " | messageType=" + handshakeType
        );

        Message handshake =
                messageParser.createEmptyMessage();

        handshake.setMessageType(handshakeType);

        handshake.setOperation(
                Operation.HANDSHAKE
        );

        messageParser.putPayloadField(
                handshake,
                "body",
                body
        );

        return handshake;
    }


    // ---------------------------------------------------------
    // SEND HANDSHAKE REQUEST
    // ---------------------------------------------------------

    private CompletionStage<Void> sendHandshake(
            ClientConnection connection
    ) {

        log.debug(
                "[TCPControlProcessor][sendHandshake] Creating and sending handshake request"
        );

        Message handshakeRequest =
                createHandshakeMessage(
                        MessageType.HANDSHAKE_REQUEST,
                        "Handshake Request"
                );

        return processOutgoing(
                handshakeRequest,
                connection
        );
    }


    // ---------------------------------------------------------
    // SEND HANDSHAKE RESPONSE
    // ---------------------------------------------------------

    private void sendHandshakeResponse(
            ClientConnection connection,
            boolean accepted,
            String peerId
    ) {

        String responseBody = accepted
                ? HANDSHAKE_ACCEPTED
                : HANDSHAKE_REJECTED;

        log.debug(() ->
                "[TCPControlProcessor][sendHandshakeResponse] Sending handshake response"
                        + " | peerId=" + peerId
                        + " | accepted=" + accepted
        );

        Message handshakeResponse =
                createHandshakeMessage(
                        MessageType.HANDSHAKE_RESPONSE,
                        responseBody
                );

        processOutgoing(
                handshakeResponse,
                connection
        ).whenComplete((ignored, exception) -> {

            if (exception != null) {

                if (accepted) {

                    activePeerConnections.remove(
                            peerId,
                            connection
                    );
                }

                log.error(
                        "[TCPControlProcessor][sendHandshakeResponse] Unable to send handshake response"
                                + " | peerId=" + peerId
                                + " | accepted=" + accepted,
                        exception
                );

                closeConnection(connection);

                return;
            }

            if (accepted) {

                log.info(() ->
                        "[TCPControlProcessor][sendHandshakeResponse] Handshake accepted"
                                + " | peerId=" + peerId
                );

            } else {

                log.warn(() ->
                        "[TCPControlProcessor][sendHandshakeResponse] Handshake rejected"
                                + " | peerId=" + peerId
                );

                closeConnection(connection);
            }
        });
    }

    private static String normalizePeerId(String peerId) {

        if (peerId == null || peerId.isBlank()) {
            return null;
        }

        return peerId.trim().toUpperCase(Locale.ROOT);
    }


    // ---------------------------------------------------------
    // RESOLVE INCOMING HANDSHAKE REQUEST
    // ---------------------------------------------------------

    private void resolveHandshakeRequest(
            Message handshake,
            ClientConnection clientConnection
    ) {

        String incomingPeerId = handshake.getProcessName();

        log.debug(() ->
                "[TCPControlProcessor][resolveHandshakeRequest] Resolving incoming handshake"
                        + " | peerId=" + incomingPeerId
        );

        if (handshake.getOperation() != Operation.HANDSHAKE) {

            log.warn(() ->
                    "[TCPControlProcessor][resolveHandshakeRequest] Invalid handshake operation"
                            + " | peerId=" + incomingPeerId
                            + " | operation=" + handshake.getOperation()
            );

            sendHandshakeResponse(
                    clientConnection,
                    false,
                    handshake.getProcessName()
            );

            return;
        }


        String peerId = normalizePeerId(handshake.getProcessName());

        if (peerId == null || peerId.isBlank()) {

            log.warn(
                    "[TCPControlProcessor][resolveHandshakeRequest] Handshake rejected because peer ID is missing"
            );

            sendHandshakeResponse(
                    clientConnection,
                    false,
                    null
            );

            return;
        }


        /*
         * Only configured peers are allowed to register
         * themselves with this processor.
         */
        if (!isConfiguredPeer(peerId)) {

            log.warn(() ->
                    "[TCPControlProcessor][resolveHandshakeRequest] Unknown peer attempted handshake"
                            + " | peerId=" + peerId
            );

            sendHandshakeResponse(
                    clientConnection,
                    false,
                    peerId
            );

            return;
        }


        /*
         * Register the connection before sending Accepted.
         *
         * This avoids a race where the remote process receives
         * the acceptance and immediately sends its first request
         * before this side has registered the connection.
         */
        boolean registered =
                registerActivePeer(
                        peerId,
                        clientConnection
                );

        if (!registered) {

            log.warn(() ->
                    "[TCPControlProcessor][resolveHandshakeRequest] Duplicate active connection attempt from peer"
                            + " | peerId=" + peerId
            );

            sendHandshakeResponse(
                    clientConnection,
                    false,
                    peerId
            );

            return;
        }

        log.info(() ->
                "[TCPControlProcessor][resolveHandshakeRequest] Configured peer registered; sending handshake acceptance"
                        + " | peerId=" + peerId
                        + " | activePeerConnections=" + activePeerConnections.size()
        );

        sendHandshakeResponse(
                clientConnection,
                true,
                peerId
        );
    }


    // ---------------------------------------------------------
    // RESOLVE OUTGOING HANDSHAKE RESPONSE
    // ---------------------------------------------------------

    private void resolveHandshakeResponse(
            Message handshake,
            ClientConnection clientConnection
    ) {

        log.debug(() ->
                "[TCPControlProcessor][resolveHandshakeResponse] Resolving outgoing handshake response"
                        + " | peerId=" + handshake.getProcessName()
        );

        if (handshake.getOperation() != Operation.HANDSHAKE) {

            log.warn(() ->
                    "[TCPControlProcessor][resolveHandshakeResponse] Invalid handshake response operation"
                            + " | peerId=" + handshake.getProcessName()
                            + " | operation=" + handshake.getOperation()
            );

            rejectPendingHandshake(
                    normalizePeerId(handshake.getProcessName()),
                    clientConnection
            );

            return;
        }

        String peerId = normalizePeerId(handshake.getProcessName());

        if (peerId == null) {

            log.warn(
                    "[TCPControlProcessor][resolveHandshakeResponse] Handshake response has no peer ID"
            );

            closeConnection(clientConnection);
            return;
        }

        PendingHandshake pending =
                pendingPeerConnections.get(peerId);

        if (pending == null ||
                pending.connection() != clientConnection) {

            log.warn(() ->
                    "[TCPControlProcessor][resolveHandshakeResponse] Unexpected handshake response"
                            + " | peerId=" + peerId
                            + " | pendingPeers=" + pendingPeerConnections.keySet()
            );

            closeConnection(clientConnection);
            return;
        }

        String responseBody;

        try {

            responseBody =
                    messageParser.extractPayloadField(
                            handshake,
                            "body",
                            String.class
                    );

        } catch (JsonProcessingException exception) {

            log.error(
                    "[TCPControlProcessor][resolveHandshakeResponse] Unable to decode handshake response"
                            + " | peerId=" + peerId,
                    exception
            );

            rejectPendingHandshake(peerId, clientConnection);
            return;
        }

        if (!HANDSHAKE_ACCEPTED.equals(responseBody)) {

            rejectPendingHandshake(peerId, clientConnection);

            log.warn(() ->
                    "[TCPControlProcessor][resolveHandshakeResponse] Peer rejected handshake"
                            + " | peerId=" + peerId
                            + " | response=" + responseBody
            );

            return;
        }

        if (!registerActivePeer(peerId, clientConnection)) {

            rejectPendingHandshake(peerId, clientConnection);

            log.warn(() ->
                    "[TCPControlProcessor][resolveHandshakeResponse] Another active connection already exists for peer"
                            + " | peerId=" + peerId
            );

            return;
        }

        pendingPeerConnections.remove(peerId, pending);

        log.info(() ->
                "[TCPControlProcessor][resolveHandshakeResponse] Peer handshake completed successfully"
                        + " | peerId=" + peerId
                        + " | activePeerConnections=" + activePeerConnections.size()
                        + " | pendingPeerConnections=" + pendingPeerConnections.size()
        );
    }


    private void rejectPendingHandshake(
            String peerId,
            ClientConnection clientConnection
    ) {

        if (peerId == null) {
            return;
        }

        PendingHandshake pending =
                pendingPeerConnections.get(peerId);

        if (pending != null &&
                pending.connection() == clientConnection) {

            pendingPeerConnections.remove(
                    peerId,
                    pending
            );

            log.debug(() ->
                    "[TCPControlProcessor][rejectPendingHandshake] Pending handshake registration removed"
                            + " | peerId=" + peerId
            );
        }

        closeConnection(clientConnection);
    }


    // ---------------------------------------------------------
    // PEER RECONCILIATION
    // ---------------------------------------------------------


    @Override
    public synchronized void reconcilePeers() {

        log.debug(() ->
                "[TCPControlProcessor][reconcilePeers] Starting peer reconciliation"
                        + " | connectionMode=" + connectionMode
                        + " | configuredPeers=" + configuredPeers.size()
                        + " | activePeers=" + activePeerConnections.size()
                        + " | pendingHandshakes=" + pendingPeerConnections.size()
        );

        if (connectionMode == TCPConnectionMode.SERVER_ONLY) {

            log.debug(
                    "[TCPControlProcessor][reconcilePeers] Peer reconciliation skipped because connection mode is SERVER_ONLY"
            );

            return;
        }


        TCPService service = tcpService;

        if (service == null) {

            log.error(
                    "[TCPControlProcessor][reconcilePeers] TCP service has not been initialized"
            );

            return;
        }


        /*
         * Remove pending handshakes that have exceeded
         * their timeout before attempting new connections.
         */
        cleanupExpiredHandshakes();


        for (TCPPeer peer : configuredPeers) {

            String peerId = normalizePeerId(peer.processId());

            if (peerId == null) {
                continue;
            }

            log.debug(() ->
                    "[TCPControlProcessor][reconcilePeers] Evaluating configured peer"
                            + " | peerId=" + peerId
                            + " | host=" + peer.host()
                            + " | port=" + peer.port()
            );


            // ---------------------------------------------
            // Check existing active connection
            // ---------------------------------------------

            ClientConnection active =
                    activePeerConnections.get(peerId);

            if (active != null) {

                if (isConnectionUsable(active)) {

                    log.debug(() ->
                            "[TCPControlProcessor][reconcilePeers] Peer already has a usable active connection"
                                    + " | peerId=" + peerId
                    );

                    continue;
                }

                activePeerConnections.remove(
                        peerId,
                        active
                );

                log.warn(() ->
                        "[TCPControlProcessor][reconcilePeers] Removed unusable active peer connection"
                                + " | peerId=" + peerId
                );
            }


            // ---------------------------------------------
            // Check pending handshake
            // ---------------------------------------------

            if (pendingPeerConnections.containsKey(peerId)) {

                log.debug(() ->
                        "[TCPControlProcessor][reconcilePeers] Connection attempt skipped because handshake is already pending"
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
                        "[TCPControlProcessor][reconcilePeers] Attempting TCP connection to peer"
                                + " | peerId=" + peerId
                                + " | host=" + peer.host()
                                + " | port=" + peer.port()
                );

                connection = service.attemptConnection(peer);

            } catch (Exception exception) {

                log.error(
                        "[TCPControlProcessor][reconcilePeers] Connection attempt failed"
                                + " | peerId=" + peerId,
                        exception
                );

                continue;
            }


            if (connection == null) {

                log.debug(() ->
                        "[TCPControlProcessor][reconcilePeers] TCP service returned no connection"
                                + " | peerId=" + peerId
                );

                continue;
            }


            // ---------------------------------------------
            // Register pending connection
            // ---------------------------------------------

            PendingHandshake pending =
                    new PendingHandshake(
                            connection,
                            System.nanoTime()
                    );

            PendingHandshake existing =
                    pendingPeerConnections.putIfAbsent(
                            peerId,
                            pending
                    );

            if (existing != null) {

                log.warn(() ->
                        "[TCPControlProcessor][reconcilePeers] Another pending handshake already exists; closing duplicate connection"
                                + " | peerId=" + peerId
                );

                closeConnection(connection);
                continue;
            }

            log.debug(() ->
                    "[TCPControlProcessor][reconcilePeers] Pending handshake registered"
                            + " | peerId=" + peerId
                            + " | pendingHandshakes=" + pendingPeerConnections.size()
            );


            // ---------------------------------------------
            // Send handshake
            // ---------------------------------------------

            sendHandshake(connection)
                    .whenComplete((ignored, exception) -> {

                        if (exception != null) {

                            pendingPeerConnections.remove(
                                    peerId,
                                    pending
                            );

                            log.error(
                                    "[TCPControlProcessor][reconcilePeers] Handshake request delivery failed"
                                            + " | peerId=" + peerId,
                                    exception
                            );

                            closeConnection(connection);
                        }
                    });
        }

        log.debug(() ->
                "[TCPControlProcessor][reconcilePeers] Peer reconciliation completed"
                        + " | activePeers=" + activePeerConnections.size()
                        + " | pendingHandshakes=" + pendingPeerConnections.size()
        );
    }


    // ---------------------------------------------------------
    // PENDING HANDSHAKE CLEANUP
    // ---------------------------------------------------------

    private void cleanupExpiredHandshakes() {

        long now = System.nanoTime();

        int removedHandshakes = 0;

        for (Map.Entry<String, PendingHandshake> entry :
                pendingPeerConnections.entrySet()) {

            String peerId = entry.getKey();

            PendingHandshake pending = entry.getValue();

            long elapsed =
                    now - pending.initiatedAtNanos();

            if (elapsed < HANDSHAKE_TIMEOUT_NANOS) {
                continue;
            }


            if (pendingPeerConnections.remove(
                    peerId,
                    pending
            )) {

                removedHandshakes++;

                log.warn(() ->
                        "[TCPControlProcessor][cleanupExpiredHandshakes] Handshake timed out"
                                + " | peerId=" + peerId
                                + " | timeoutMillis=" + HANDSHAKE_TIMEOUT_MILLIS
                );

                closeConnection(
                        pending.connection()
                );
            }
        }

        final int expiredHandshakeCount = removedHandshakes;

        log.debug(() ->
                "[TCPControlProcessor][cleanupExpiredHandshakes] Pending handshake cleanup completed"
                        + " | removedHandshakes=" + expiredHandshakeCount
                        + " | pendingHandshakes=" + pendingPeerConnections.size()
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
                "[TCPControlProcessor][processOutgoing] Routing outgoing control message"
                        + " | messageType=" + outgoing.getMessageType()
                        + " | operation=" + outgoing.getOperation()
                        + " | correlationId=" + outgoing.getCorrelationId()
                        + " | recipientId=" + outgoing.getRecipientID()
        );


        // ---------------------------------------------
        // RESPONSE: use correlated reply channel
        // ---------------------------------------------

        if (outgoing.getMessageType() == MessageType.RESPONSE) {

            UUID correlationId =
                    outgoing.getCorrelationId();

            if (correlationId == null) {

                log.warn(
                        "[TCPControlProcessor][processOutgoing] Response rejected because correlation ID is null"
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
                        "[TCPControlProcessor][processOutgoing] Final response; removing control correlation"
                                + " | correlationId=" + correlationId
                );

                replyChannel =
                        tcpControlRegistry.removeAndGet(
                                correlationId
                        );

            } else {

                log.debug(() ->
                        "[TCPControlProcessor][processOutgoing] Intermediate response; retaining control correlation"
                                + " | correlationId=" + correlationId
                );

                replyChannel =
                        tcpControlRegistry.get(
                                correlationId
                        );

                tcpControlRegistry.resetTimeout(
                        correlationId
                );
            }


            if (replyChannel == null) {

                log.warn(() ->
                        "[TCPControlProcessor][processOutgoing] No pending reply channel found"
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
                    "[TCPControlProcessor][processOutgoing] Dispatching response through correlated reply channel"
                            + " | correlationId=" + correlationId
            );

            return replyChannel.sendReply(outgoing);
        }


        // ---------------------------------------------
        // Other message types: route by recipient ID
        // ---------------------------------------------

        String recipientId =
                normalizePeerId(outgoing.getRecipientID());

        if (recipientId == null || recipientId.isBlank()) {

            log.warn(
                    "[TCPControlProcessor][processOutgoing] Outgoing message rejected because recipient ID is missing"
            );

            return CompletableFuture.failedFuture(
                    new IllegalArgumentException(
                            "Outgoing message recipient ID is missing"
                    )
            );
        }


        ClientConnection connection =
                activePeerConnections.get(recipientId);

        if (connection == null) {

            log.warn(() ->
                    "[TCPControlProcessor][processOutgoing] No active connection found for recipient"
                            + " | recipientId=" + recipientId
                            + " | activePeerConnections=" + activePeerConnections.size()
            );

            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "No active connection for peer: " + recipientId
                    )
            );
        }


        if (!isConnectionUsable(connection)) {

            activePeerConnections.remove(
                    recipientId,
                    connection
            );

            closeConnection(connection);

            log.warn(() ->
                    "[TCPControlProcessor][processOutgoing] Recipient connection is no longer usable"
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
                    "[TCPControlProcessor][processOutgoing] TCP service has not been initialized"
            );

            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "TCP service has not been initialized"
                    )
            );
        }

        log.debug(() ->
                "[TCPControlProcessor][processOutgoing] Writing message to active peer connection"
                        + " | recipientId=" + recipientId
                        + " | messageType=" + outgoing.getMessageType()
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
                "[TCPControlProcessor][processOutgoing] Sending message through supplied reply channel"
                        + " | messageType=" + outgoing.getMessageType()
                        + " | correlationId=" + outgoing.getCorrelationId()
                        + " | replyChannel=" + replyChannel.getClass().getSimpleName()
        );

        return replyChannel.sendReply(outgoing);
    }


    // ---------------------------------------------------------
    // CORRELATION CLEANUP
    // ---------------------------------------------------------

    public void cleanupExpiredCorrelations() {

        log.debug(() ->
                "[TCPControlProcessor][cleanupExpiredCorrelations] Starting control correlation cleanup"
                        + " | currentCorrelations=" + tcpControlRegistry.getSize()
                        + " | timeoutMillis=" + CORRELATION_TIMEOUT_MILLIS
        );

        int expiredCount = 0;

        for (TCPControlRegistry.ExpiredCorrelation expired :
                tcpControlRegistry.removeExpired(
                        CORRELATION_TIMEOUT_MILLIS
                )) {

            expiredCount++;

            log.warn(() ->
                    "[TCPControlProcessor][cleanupExpiredCorrelations] Control request correlation expired"
                            + " | correlationId=" + expired.correlationId()
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
                                        "[TCPControlProcessor][cleanupExpiredCorrelations] Timeout reply delivery failed"
                                                + " | correlationId=" + expired.correlationId(),
                                        exception
                                );
                            }
                        });

            } catch (Exception exception) {

                log.error(
                        "[TCPControlProcessor][cleanupExpiredCorrelations] Timeout cleanup failed"
                                + " | correlationId=" + expired.correlationId(),
                        exception
                );
            }
        }

        final int expiredCorrelationCount = expiredCount;

        log.debug(() ->
                "[TCPControlProcessor][cleanupExpiredCorrelations] Control correlation cleanup completed"
                        + " | expiredCorrelations=" + expiredCorrelationCount
                        + " | remainingCorrelations=" + tcpControlRegistry.getSize()
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
                "[TCPControlProcessor][sendRequest] Sending control request"
                        + " | correlationId=" + request.getCorrelationId()
                        + " | recipientId=" + request.getRecipientID()
        );

        return processOutgoing(
                request
        );
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
                "[TCPControlProcessor][sendRequest] Registering reply channel for outgoing control request"
                        + " | correlationId=" + correlationId
        );


        tcpControlRegistry.add(
                correlationId,
                replyChannel
        );


        return processOutgoing(request)
                .whenComplete((ignored, exception) -> {

                    if (exception != null) {

                        tcpControlRegistry.removeAndGet(
                                correlationId
                        );

                        log.error(
                                "[TCPControlProcessor][sendRequest] Control request delivery failed; correlation removed"
                                        + " | correlationId=" + correlationId,
                                exception
                        );
                    } else {

                        log.debug(() ->
                                "[TCPControlProcessor][sendRequest] Control request submitted successfully"
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
                "[TCPControlProcessor][sendResponse] Sending control response"
                        + " | correlationId=" + response.getCorrelationId()
                        + " | messageType=" + response.getMessageType()
        );

        processOutgoing(response)
                .whenComplete((ignored, exception) -> {

                    if (exception != null) {

                        log.error(
                                "[TCPControlProcessor][sendResponse] Unable to send control response"
                                        + " | correlationId=" + response.getCorrelationId(),
                                exception
                        );
                    }
                });
    }


    @Override
    public void disconnect(
            String connectionName
    ) {

        if (connectionName == null || connectionName.isBlank()) {
            return;
        }

        log.info(() ->
                "[TCPControlProcessor][disconnect] Disconnecting peer"
                        + " | peerId=" + connectionName
        );


        ClientConnection active =
                activePeerConnections.remove(connectionName);

        PendingHandshake pending =
                pendingPeerConnections.remove(connectionName);


        if (active != null) {
            closeConnection(active);
        }

        if (pending != null) {
            closeConnection(pending.connection());
        }


        log.info(() ->
                "[TCPControlProcessor][disconnect] Peer disconnected"
                        + " | peerId=" + connectionName
                        + " | activeConnectionRemoved=" + (active != null)
                        + " | pendingHandshakeRemoved=" + (pending != null)
                        + " | activePeerConnections=" + activePeerConnections.size()
                        + " | pendingPeerConnections=" + pendingPeerConnections.size()
        );
    }


    // ---------------------------------------------------------
    // TCP PROCESSOR LIFECYCLE
    // ---------------------------------------------------------

    @Override
    public void cleanupCorrelations() {

        log.debug(
                "[TCPControlProcessor][cleanupCorrelations] TCP control cleanup cycle started"
        );

        cleanupExpiredCorrelations();

        cleanupExpiredHandshakes();

        log.debug(
                "[TCPControlProcessor][cleanupCorrelations] TCP control cleanup cycle completed"
        );
    }


    @Override
    public void informShutdown() {

        log.info(() ->
                "[TCPControlProcessor][informShutdown] TCP control processor shutdown initiated"
                        + " | activePeers=" + activePeerConnections.size()
                        + " | pendingHandshakes=" + pendingPeerConnections.size()
                        + " | correlations=" + tcpControlRegistry.getSize()
        );


        // ---------------------------------------------
        // Fail outstanding correlated requests
        // ---------------------------------------------

        for (TCPControlRegistry.ExpiredCorrelation expired :
                tcpControlRegistry.removeExpired(0)) {

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
                                        "[TCPControlProcessor][informShutdown] Shutdown reply delivery failed"
                                                + " | correlationId=" + expired.correlationId(),
                                        exception
                                );
                            }
                        });

            } catch (Exception exception) {

                log.error(
                        "[TCPControlProcessor][informShutdown] Shutdown notification failed"
                                + " | correlationId=" + expired.correlationId(),
                        exception
                );
            }
        }


        // ---------------------------------------------
        // Close active connections
        // ---------------------------------------------

        for (ClientConnection connection :
                activePeerConnections.values()) {

            closeConnection(connection);
        }

        activePeerConnections.clear();


        // ---------------------------------------------
        // Close pending handshake connections
        // ---------------------------------------------

        for (PendingHandshake pending :
                pendingPeerConnections.values()) {

            closeConnection(
                    pending.connection()
            );
        }

        pendingPeerConnections.clear();


        log.info(
                "[TCPControlProcessor][informShutdown] TCP control processor shutdown cleanup completed"
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
                "[TCPControlProcessor][onConnectionClosed] Connection closure notification received"
        );

        // Remove every active peer mapping using this connection.
        activePeerConnections.forEach((peerId, connection) -> {

            if (connection == closedConnection &&
                    activePeerConnections.remove(peerId, connection)) {

                log.info(() ->
                        "[TCPControlProcessor][onConnectionClosed] Removed disconnected peer"
                                + " | peerId=" + peerId
                                + " | activePeerConnections=" + activePeerConnections.size()
                );
            }
        });


        // Also remove any pending handshake using this connection.
        pendingPeerConnections.forEach((peerId, pending) -> {

            if (pending.connection() == closedConnection &&
                    pendingPeerConnections.remove(peerId, pending)) {

                log.debug(() ->
                        "[TCPControlProcessor][onConnectionClosed] Removed pending handshake"
                                + " | peerId=" + peerId
                                + " | pendingPeerConnections=" + pendingPeerConnections.size()
                );
            }
        });
    }
}
