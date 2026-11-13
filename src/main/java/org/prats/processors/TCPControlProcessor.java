
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

import org.prats.loggers.FileLogger;
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
    }


    // ---------------------------------------------------------
    // DEPENDENCY INJECTION
    // ---------------------------------------------------------

    public void setTcpService(TCPService tcpService) {

        this.tcpService = Objects.requireNonNull(
                tcpService,
                "TCP service cannot be null"
        );
    }


    public void setControlMessageHandler(
            ControlMessageHandler controlMessageHandler
    ) {

        this.controlMessageHandler = Objects.requireNonNull(
                controlMessageHandler,
                "Control message handler cannot be null"
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
            throw new IllegalArgumentException(
                    "Peer ID cannot be blank"
            );
        }

        if (!registerActivePeer(peerId, connection)) {

            throw new IllegalStateException(
                    "A different active connection already exists for peer: "
                            + peerId
            );
        }
    }


    private boolean registerActivePeer(
            String peerId,
            ClientConnection connection
    ) {

        Objects.requireNonNull(peerId);
        Objects.requireNonNull(connection);

        peerId = normalizePeerId(peerId);

        if (peerId == null) {
            return false;
        }

        while (true) {

            ClientConnection existing =
                    activePeerConnections.putIfAbsent(
                            peerId,
                            connection
                    );

            if (existing == null || existing == connection) {
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

                    return true;
                }

                continue;
            }

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

        activePeerConnections.remove(
                peerId,
                connection
        );

        PendingHandshake pending =
                pendingPeerConnections.get(peerId);

        if (pending != null &&
                pending.connection() == connection) {

            pendingPeerConnections.remove(
                    peerId,
                    pending
            );
        }
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

        return configuredPeers.stream()
                .map(TCPPeer::processId)
                .map(TCPControlProcessor::normalizePeerId)
                .anyMatch(normalizedPeerId::equals);
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

        return registeredConnection == connection
                && isConnectionUsable(connection);
    }


    private void closeConnection(
            ClientConnection connection
    ) {

        if (connection == null) {
            return;
        }

        try {

            connection.getSocket().close();

        } catch (IOException exception) {

            FileLogger.log(
                    "[TCPControlProcessor] Unable to close connection: "
                            + exception.getMessage()
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
            return;
        }

        Objects.requireNonNull(
                clientConnection,
                "Client connection cannot be null"
        );

        Message incomingMessage;

        try {

            incomingMessage = Objects.requireNonNull(
                    messageParser.createMessageFromJSON(rawMessage),
                    "Parsed control message cannot be null"
            );

        } catch (Exception exception) {

            FileLogger.log(
                    "[TCPControlProcessor] Message parsing failed: "
                            + exception.getMessage()
            );

            return;
        }


        try {

            MessageType messageType =
                    Objects.requireNonNull(
                            incomingMessage.getMessageType(),
                            "Message type cannot be null"
                    );


            switch (messageType) {

                // ---------------------------------------------
                // HANDSHAKE
                // ---------------------------------------------

                case HANDSHAKE_REQUEST -> {

                    resolveHandshakeRequest(
                            incomingMessage,
                            clientConnection
                    );
                }


                case HANDSHAKE_RESPONSE -> {

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

                        FileLogger.log(
                                "[TCPControlProcessor] "
                                        + "Response received from "
                                        + "an unregistered peer."
                        );

                        return;
                    }

                    requireControlMessageHandler()
                            .onResponse(incomingMessage);
                }


                // ---------------------------------------------
                // HEARTBEAT
                // ---------------------------------------------

                case HEARTBEAT -> {

                    if (!isRegisteredPeerConnection(
                            incomingMessage,
                            clientConnection
                    )) {

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

                        return;
                    }

                    requireControlMessageHandler()
                            .onInfo(incomingMessage);
                }


                // ---------------------------------------------
                // UNKNOWN MESSAGE TYPE
                // ---------------------------------------------

                default -> {

                    FileLogger.log(
                            "[TCPControlProcessor] Unsupported message type: "
                                    + messageType
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

            FileLogger.log(
                    "[TCPControlProcessor] Control message processing failed: "
                            + exception
            );
        }
    }


    private ControlMessageHandler requireControlMessageHandler() {

        return Objects.requireNonNull(
                controlMessageHandler,
                "Control message handler has not been initialized"
        );
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

            FileLogger.log(
                    "[TCPControlProcessor] Received request without "
                            + "correlation ID."
            );

            return;
        }


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

            requireControlMessageHandler()
                    .onRequest(incomingMessage);

        } catch (Exception exception) {

            tcpControlRegistry.removeAndGet(correlationId);

            FileLogger.log(
                    "[TCPControlProcessor] Request handler failed: "
                            + exception
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

                FileLogger.log(
                        "[TCPControlProcessor] Unable to send protocol error: "
                                + exception.getMessage()
                );
            }
        });
    }


    // ---------------------------------------------------------
    // SERIALIZATION
    // ---------------------------------------------------------

    @Override
    public String serialize(Message message) {

        return messageParser.createJSONFromMessage(
                Objects.requireNonNull(message)
        );
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

        try {

            clientConnection.sendMessage(
                    serialize(outgoing)
            );

            return CompletableFuture.completedFuture(null);

        } catch (Exception exception) {

            FileLogger.log(
                    "[TCPControlProcessor] Direct socket write failed: "
                            + exception.getMessage()
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

                FileLogger.log(
                        "[TCPControlProcessor] Unable to send handshake "
                                + "response to peer: " + peerId
                                + " | " + exception.getMessage()
                );

                closeConnection(connection);

                return;
            }

            if (accepted) {

                FileLogger.log(
                        "[TCPControlProcessor] Handshake accepted: "
                                + peerId
                );

            } else {

                FileLogger.log(
                        "[TCPControlProcessor] Handshake rejected: "
                                + peerId
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

        if (handshake.getOperation() != Operation.HANDSHAKE) {

            FileLogger.log(
                    "[TCPControlProcessor] Invalid handshake operation."
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

            FileLogger.log(
                    "[TCPControlProcessor] Unknown peer attempted "
                            + "handshake: " + peerId
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

            FileLogger.log(
                    "[TCPControlProcessor] Duplicate active connection "
                            + "attempt from peer: " + peerId
            );

            sendHandshakeResponse(
                    clientConnection,
                    false,
                    peerId
            );

            return;
        }


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

        if (handshake.getOperation() != Operation.HANDSHAKE) {

            FileLogger.log(
                    "[TCPControlProcessor] Invalid handshake response operation."
            );

            rejectPendingHandshake(
                    normalizePeerId(handshake.getProcessName()),
                    clientConnection
            );

            return;
        }

        String peerId = normalizePeerId(handshake.getProcessName());

        if (peerId == null) {

            FileLogger.log(
                    "[TCPControlProcessor] Handshake response has no peer ID."
            );

            closeConnection(clientConnection);
            return;
        }

        PendingHandshake pending =
                pendingPeerConnections.get(peerId);

        if (pending == null ||
                pending.connection() != clientConnection) {

            FileLogger.log(
                    "[TCPControlProcessor] Unexpected handshake response "
                            + "from peer: " + peerId
                            + " | Pending peers: "
                            + pendingPeerConnections.keySet()
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

            FileLogger.log(
                    "[TCPControlProcessor] Unable to decode handshake "
                            + "response from: " + peerId
            );

            rejectPendingHandshake(peerId, clientConnection);
            return;
        }

        if (!HANDSHAKE_ACCEPTED.equals(responseBody)) {

            rejectPendingHandshake(peerId, clientConnection);

            FileLogger.log(
                    "[TCPControlProcessor] Peer rejected handshake: "
                            + peerId
                            + " | Response: " + responseBody
            );

            return;
        }

        if (!registerActivePeer(peerId, clientConnection)) {

            rejectPendingHandshake(peerId, clientConnection);

            FileLogger.log(
                    "[TCPControlProcessor] Another active connection "
                            + "already exists for peer: " + peerId
            );

            return;
        }

        pendingPeerConnections.remove(peerId, pending);

        FileLogger.log(
                "[TCPControlProcessor] Peer handshake completed: "
                        + peerId
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
        }

        closeConnection(clientConnection);
    }


    // ---------------------------------------------------------
    // PEER RECONCILIATION
    // ---------------------------------------------------------


    @Override
    public synchronized void reconcilePeers() {



        if (connectionMode == TCPConnectionMode.SERVER_ONLY) {
            return;
        }


        TCPService service = tcpService;

        if (service == null) {

            FileLogger.log(
                    "[TCPControlProcessor] TCP service has not been initialized."
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


            // ---------------------------------------------
            // Check existing active connection
            // ---------------------------------------------

            ClientConnection active =
                    activePeerConnections.get(peerId);

            if (active != null) {

                if (isConnectionUsable(active)) {
                    continue;
                }

                activePeerConnections.remove(
                        peerId,
                        active
                );
            }


            // ---------------------------------------------
            // Check pending handshake
            // ---------------------------------------------

            if (pendingPeerConnections.containsKey(peerId)) {
                continue;
            }


            // ---------------------------------------------
            // Establish TCP connection
            // ---------------------------------------------

            ClientConnection connection;

            try {

                connection = service.attemptConnection(peer);

            } catch (Exception exception) {

                FileLogger.log(
                        "[TCPControlProcessor] Connection attempt failed "
                                + "for peer: " + peerId
                                + " | " + exception.getMessage()
                );

                continue;
            }


            if (connection == null) {
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

                closeConnection(connection);
                continue;
            }


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

                            FileLogger.log(
                                    "[TCPControlProcessor] Handshake "
                                            + "request delivery failed for "
                                            + peerId + " | " + exception
                            );

                            closeConnection(connection);
                        }
                    });
        }
    }


    // ---------------------------------------------------------
    // PENDING HANDSHAKE CLEANUP
    // ---------------------------------------------------------

    private void cleanupExpiredHandshakes() {

        long now = System.nanoTime();

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

                FileLogger.log(
                        "[TCPControlProcessor] Handshake timed out "
                                + "for peer: " + peerId
                );

                closeConnection(
                        pending.connection()
                );
            }
        }
    }


    // ---------------------------------------------------------
    // OUTGOING MESSAGE ROUTING
    // ---------------------------------------------------------

    public CompletionStage<Void> processOutgoing(
            Message outgoing
    ) {

        Objects.requireNonNull(outgoing);


        // ---------------------------------------------
        // RESPONSE: use correlated reply channel
        // ---------------------------------------------

        if (outgoing.getMessageType() == MessageType.RESPONSE) {

            UUID correlationId =
                    outgoing.getCorrelationId();

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
                        tcpControlRegistry.removeAndGet(
                                correlationId
                        );

            } else {

                replyChannel =
                        tcpControlRegistry.get(
                                correlationId
                        );

                tcpControlRegistry.resetTimeout(
                        correlationId
                );
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


        // ---------------------------------------------
        // Other message types: route by recipient ID
        // ---------------------------------------------

        String recipientId =
                normalizePeerId(outgoing.getRecipientID());

        if (recipientId == null || recipientId.isBlank()) {

            return CompletableFuture.failedFuture(
                    new IllegalArgumentException(
                            "Outgoing message recipient ID is missing"
                    )
            );
        }


        ClientConnection connection =
                activePeerConnections.get(recipientId);

        if (connection == null) {

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

            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "Connection is no longer usable for peer: "
                                    + recipientId
                    )
            );
        }


        TCPService service = tcpService;

        if (service == null) {

            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "TCP service has not been initialized"
                    )
            );
        }


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

        return replyChannel.sendReply(outgoing);
    }


    // ---------------------------------------------------------
    // CORRELATION CLEANUP
    // ---------------------------------------------------------

    public void cleanupExpiredCorrelations() {

        for (TCPControlRegistry.ExpiredCorrelation expired :
                tcpControlRegistry.removeExpired(
                        CORRELATION_TIMEOUT_MILLIS
                )) {

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

                                FileLogger.log(
                                        "[TCPControlProcessor] Timeout "
                                                + "reply delivery failed: "
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


    // ---------------------------------------------------------
    // CONTROL CHANNEL COMMUNICATOR
    // ---------------------------------------------------------

    @Override
    public CompletionStage<Void> sendRequest(
            Message request
    ) {

        return processOutgoing(
                Objects.requireNonNull(request)
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
                    }
                });
    }


    @Override
    public void sendResponse(
            Message response
    ) {

        processOutgoing(response)
                .whenComplete((ignored, exception) -> {

                    if (exception != null) {

                        FileLogger.log(
                                "[TCPControlProcessor] Unable to send "
                                        + "response: "
                                        + exception.getMessage()
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


        FileLogger.log(
                "[TCPControlProcessor] Disconnected peer: "
                        + connectionName
        );
    }


    // ---------------------------------------------------------
    // TCP PROCESSOR LIFECYCLE
    // ---------------------------------------------------------

    @Override
    public void cleanupCorrelations() {

        cleanupExpiredCorrelations();

        cleanupExpiredHandshakes();
    }


    @Override
    public void informShutdown() {

        FileLogger.log(
                "[TCPControlProcessor] Shutdown initiated."
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

                                FileLogger.log(
                                        "[TCPControlProcessor] Shutdown "
                                                + "reply delivery failed: "
                                                + exception.getMessage()
                                );
                            }
                        });

            } catch (Exception exception) {

                FileLogger.log(
                        "[TCPControlProcessor] Shutdown notification "
                                + "failed: " + exception.getMessage()
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


        FileLogger.log(
                "[TCPControlProcessor] Shutdown cleanup completed."
        );
    }

    @Override
    public void onConnectionClosed(
            ClientConnection closedConnection
    ) {

        if (closedConnection == null) {
            return;
        }

        // Remove every active peer mapping using this connection.
        activePeerConnections.forEach((peerId, connection) -> {

            if (connection == closedConnection &&
                    activePeerConnections.remove(peerId, connection)) {

                FileLogger.log(
                        "[TCPControlProcessor] Removed disconnected peer: "
                                + peerId
                );
            }
        });


        // Also remove any pending handshake using this connection.
        pendingPeerConnections.forEach((peerId, pending) -> {

            if (pending.connection() == closedConnection &&
                    pendingPeerConnections.remove(peerId, pending)) {

                FileLogger.log(
                        "[TCPControlProcessor] Removed pending handshake: "
                                + peerId
                );
            }
        });
    }
}
