
package org.prats.managers;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.records.HandshakePayload;
import org.prats.definitions.records.TCPPeer;
import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public final class TCPHandshakeManager {

    private static final EngineLogger log =
            EngineLogger.getLogger(TCPHandshakeManager.class);

    // ---------------------------------------------------------
    // CONSTANTS
    // ---------------------------------------------------------

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
    // HANDSHAKE STATE
    // ---------------------------------------------------------

    /*
     * Tracks an outgoing connection that has been established
     * but has not yet received an accepted handshake response.
     */
    private record PendingHandshake(
            ClientConnection connection,
            long initiatedAtNanos
    ) {
    }

    /*
     * Successfully registered control peers.
     *
     * Keys are normalized process IDs.
     */
    private final Map<String, ClientConnection> activePeerConnections =
            new ConcurrentHashMap<>();

    /*
     * Outgoing connections awaiting handshake responses.
     */
    private final Map<String, PendingHandshake> pendingPeerConnections =
            new ConcurrentHashMap<>();

    // ---------------------------------------------------------
    // DEPENDENCIES
    // ---------------------------------------------------------

    private final MessageParser messageParser;

    /*
     * Only explicitly configured peers may be admitted.
     *
     * This preserves the TCPProtocolProcessor's original
     * handshake admission behavior.
     */
    private final Set<String> configuredPeerIds;

    /*
     * Sends a message directly over the supplied connection.
     *
     * Handshake messages must not require an already
     * registered peer connection.
     */
    private final BiFunction<
            Message,
            ClientConnection,
            CompletionStage<Void>
            > messageSender;

    // ---------------------------------------------------------
    // CONSTRUCTOR
    // ---------------------------------------------------------

    public TCPHandshakeManager(
            List<TCPPeer> configuredPeers,
            MessageParser messageParser,
            BiFunction<
                    Message,
                    ClientConnection,
                    CompletionStage<Void>
                    > messageSender
    ) {

        Objects.requireNonNull(
                configuredPeers,
                "Configured peers cannot be null"
        );

        this.messageParser = Objects.requireNonNull(
                messageParser,
                "Message parser cannot be null"
        );

        this.messageSender = Objects.requireNonNull(
                messageSender,
                "Message sender cannot be null"
        );

        Set<String> peerIds = new HashSet<>();

        for (TCPPeer peer : configuredPeers) {

            Objects.requireNonNull(
                    peer,
                    "Configured peer cannot be null"
            );

            String peerId = normalizePeerId(
                    peer.processId()
            );

            if (peerId == null) {

                throw new IllegalArgumentException(
                        "Configured peer ID cannot be blank"
                );
            }

            peerIds.add(peerId);
        }

        this.configuredPeerIds = Set.copyOf(peerIds);

        log.info(() ->
                "[TCPHandshakeManager][constructor] "
                        + "Handshake manager initialized"
                        + " | configuredPeers="
                        + configuredPeerIds.size()
                        + " | timeoutMillis="
                        + HANDSHAKE_TIMEOUT_MILLIS
        );
    }

    // ---------------------------------------------------------
    // PEER ID NORMALIZATION
    // ---------------------------------------------------------

    public static String normalizePeerId(
            String peerId
    ) {

        if (peerId == null || peerId.isBlank()) {
            return null;
        }

        return peerId.trim().toUpperCase(Locale.ROOT);
    }

    // ---------------------------------------------------------
    // CONNECTION VALIDATION
    // ---------------------------------------------------------

    /*
     * Checks whether a local socket is usable.
     *
     * This does not prove remote liveness.
     * Remote failure detection still depends on
     * reads, writes and heartbeats.
     */
    public static boolean isConnectionUsable(
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

    private void closeConnection(
            ClientConnection connection
    ) {

        if (connection == null) {
            return;
        }

        try {

            Socket socket = connection.getSocket();

            if (socket != null && !socket.isClosed()) {
                socket.close();
            }

            log.debug(
                    "[TCPHandshakeManager][closeConnection] "
                            + "Socket closed"
            );

        } catch (IOException exception) {

            log.error(
                    "[TCPHandshakeManager][closeConnection] "
                            + "Unable to close socket",
                    exception
            );
        }
    }

    // ---------------------------------------------------------
    // REGISTRY ACCESS
    // ---------------------------------------------------------

    public int getActivePeerCount() {
        return activePeerConnections.size();
    }

    public int getPendingHandshakeCount() {
        return pendingPeerConnections.size();
    }

    public List<Socket> connectedSockets() {

        return activePeerConnections.values()
                .stream()
                .map(ClientConnection::getSocket)
                .filter(Objects::nonNull)
                .filter(socket -> !socket.isClosed())
                .toList();
    }

    public boolean isPeerRegistered(
            String peerId
    ) {

        String normalized = normalizePeerId(peerId);

        return normalized != null
                && activePeerConnections.containsKey(normalized);
    }

    public boolean isPeerConnected(
            String peerId
    ) {

        return isConnectionUsable(
                getActiveConnection(peerId)
        );
    }

    public ClientConnection getActiveConnection(
            String peerId
    ) {

        String normalized = normalizePeerId(peerId);

        if (normalized == null) {
            return null;
        }

        return activePeerConnections.get(normalized);
    }

    public boolean hasPendingHandshake(
            String peerId
    ) {

        String normalized = normalizePeerId(peerId);

        return normalized != null
                && pendingPeerConnections.containsKey(normalized);
    }

    // ---------------------------------------------------------
    // PEER ADMISSION
    // ---------------------------------------------------------

    private boolean isConfiguredPeer(
            String peerId
    ) {

        String normalized = normalizePeerId(peerId);

        boolean configured =
                normalized != null
                        && configuredPeerIds.contains(normalized);

        log.debug(() ->
                "[TCPHandshakeManager][isConfiguredPeer] "
                        + "Peer admission evaluated"
                        + " | peerId=" + normalized
                        + " | configured=" + configured
        );

        return configured;
    }

    // ---------------------------------------------------------
    // ACTIVE PEER REGISTRATION
    // ---------------------------------------------------------

    /*
     * Preserves the previous control processor's
     * active-peer registration behavior.
     *
     * An existing connection can only be replaced
     * automatically if it is no longer usable.
     */
    private boolean registerActivePeer(
            String peerId,
            ClientConnection connection
    ) {

        String normalized = normalizePeerId(peerId);

        if (normalized == null || connection == null) {

            log.warn(
                    "[TCPHandshakeManager][registerActivePeer] "
                            + "Invalid peer registration"
            );

            return false;
        }

        while (true) {

            ClientConnection existing =
                    activePeerConnections.putIfAbsent(
                            normalized,
                            connection
                    );

            if (existing == null || existing == connection) {

                log.debug(() ->
                        "[TCPHandshakeManager][registerActivePeer] "
                                + "Peer registered"
                                + " | peerId=" + normalized
                                + " | activePeers="
                                + activePeerConnections.size()
                );

                return true;
            }

            if (!isConnectionUsable(existing)) {

                if (activePeerConnections.replace(
                        normalized,
                        existing,
                        connection
                )) {

                    log.info(() ->
                            "[TCPHandshakeManager][registerActivePeer] "
                                    + "Replaced unusable peer connection"
                                    + " | peerId=" + normalized
                    );

                    return true;
                }

                continue;
            }

            log.warn(() ->
                    "[TCPHandshakeManager][registerActivePeer] "
                            + "Another usable connection already exists"
                            + " | peerId=" + normalized
            );

            return false;
        }
    }

    /*
     * Compatibility method for the existing
     * TCPProtocolProcessor.registerPeerConnection() API.
     *
     * This is an explicit registration operation.
     * Normal handshake acceptance still goes through
     * the internal registration method.
     */
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

        log.info(() ->
                "[TCPHandshakeManager][registerPeerConnection] "
                        + "Peer connection explicitly registered"
                        + " | peerId=" + normalizePeerId(peerId)
        );
    }

    // ---------------------------------------------------------
    // REGISTERED CONNECTION VALIDATION
    // ---------------------------------------------------------

    /*
     * A normal control message must originate from the
     * exact connection registered for its claimed peer.
     */
    public boolean isRegisteredPeerConnection(
            Message message,
            ClientConnection connection
    ) {

        if (message == null || connection == null) {
            return false;
        }

        String peerId = normalizePeerId(
                message.getProcessName()
        );

        if (peerId == null) {
            return false;
        }

        ClientConnection registered =
                activePeerConnections.get(peerId);

        boolean accepted =
                registered == connection
                        && isConnectionUsable(connection);

        log.debug(() ->
                "[TCPHandshakeManager][isRegisteredPeerConnection] "
                        + "Peer connection evaluated"
                        + " | peerId=" + peerId
                        + " | registered=" + accepted
        );

        return accepted;
    }

    // ---------------------------------------------------------
    // HANDSHAKE MESSAGE CREATION
    // ---------------------------------------------------------

    /*
     * Uses the original MessageType and HandshakePayload.
     *
     * The wire format remains unchanged.
     */
    private Message createHandshakeMessage(
            MessageType type,
            String body
    ) {

        Message message =
                messageParser.createEmptyMessage();

        message.setMessageType(type);

        message.setOperation(
                Operation.HANDSHAKE
        );

        message.setPayload(
                messageParser.toPayload(
                        new HandshakePayload(body)
                )
        );

        return message;
    }

    // ---------------------------------------------------------
    // OUTGOING HANDSHAKE
    // ---------------------------------------------------------

    /*
     * Called after TCPService.attemptConnection().
     *
     * The connection remains pending until its
     * handshake response is accepted.
     */
    public CompletionStage<Void> initiateHandshake(
            String peerId,
            ClientConnection connection
    ) {

        String normalized = normalizePeerId(peerId);

        Objects.requireNonNull(connection);

        if (normalized == null) {

            return CompletableFuture.failedFuture(
                    new IllegalArgumentException(
                            "Peer ID cannot be blank"
                    )
            );
        }

        PendingHandshake pending =
                new PendingHandshake(
                        connection,
                        System.nanoTime()
                );

        PendingHandshake existing =
                pendingPeerConnections.putIfAbsent(
                        normalized,
                        pending
                );

        if (existing != null) {

            log.warn(() ->
                    "[TCPHandshakeManager][initiateHandshake] "
                            + "Handshake already pending"
                            + " | peerId=" + normalized
            );

            if (existing.connection() != connection) {
                closeConnection(connection);
            }

            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "Handshake already pending: " + normalized
                    )
            );
        }

        Message request =
                createHandshakeMessage(
                        MessageType.HANDSHAKE_REQUEST,
                        "Handshake Request"
                );

        log.debug(() ->
                "[TCPHandshakeManager][initiateHandshake] "
                        + "Sending handshake request"
                        + " | peerId=" + normalized
                        + " | pendingHandshakes="
                        + pendingPeerConnections.size()
        );

        try {

            CompletionStage<Void> stage =
                    Objects.requireNonNull(
                            messageSender.apply(
                                    request,
                                    connection
                            ),
                            "Handshake sender returned null"
                    );

            return stage.whenComplete((ignored, exception) -> {

                if (exception != null) {

                    pendingPeerConnections.remove(
                            normalized,
                            pending
                    );

                    log.error(
                            "[TCPHandshakeManager][initiateHandshake] "
                                    + "Handshake request delivery failed"
                                    + " | peerId=" + normalized,
                            exception
                    );

                    closeConnection(connection);
                }
            });

        } catch (RuntimeException exception) {

            pendingPeerConnections.remove(
                    normalized,
                    pending
            );

            log.error(
                    "[TCPHandshakeManager][initiateHandshake] "
                            + "Unable to initiate handshake"
                            + " | peerId=" + normalized,
                    exception
            );

            closeConnection(connection);

            return CompletableFuture.failedFuture(exception);
        }
    }

    // ---------------------------------------------------------
    // INCOMING HANDSHAKE ROUTING
    // ---------------------------------------------------------

    /*
     * Returns true only if a handshake message
     * has been recognized and processed.
     *
     * This method does not inspect ordinary
     * application or control messages.
     */
    public boolean processHandshake(
            Message message,
            ClientConnection connection
    ) {

        Objects.requireNonNull(message);
        Objects.requireNonNull(connection);

        if (message.getMessageType() ==
                MessageType.HANDSHAKE_REQUEST) {

            resolveHandshakeRequest(message, connection);
            return true;
        }

        if (message.getMessageType() ==
                MessageType.HANDSHAKE_RESPONSE) {

            resolveHandshakeResponse(message, connection);
            return true;
        }

        return false;
    }

    // ---------------------------------------------------------
    // INCOMING HANDSHAKE REQUEST
    // ---------------------------------------------------------

    private void resolveHandshakeRequest(
            Message handshake,
            ClientConnection connection
    ) {

        String peerId = normalizePeerId(
                handshake.getProcessName()
        );

        log.debug(() ->
                "[TCPHandshakeManager][resolveHandshakeRequest] "
                        + "Incoming handshake request"
                        + " | peerId=" + peerId
                        + " | operation=" + handshake.getOperation()
        );

        if (handshake.getOperation() != Operation.HANDSHAKE
                || peerId == null) {

            log.warn(
                    "[TCPHandshakeManager][resolveHandshakeRequest] "
                            + "Invalid handshake request"
            );

            sendHandshakeResponse(
                    connection,
                    false,
                    peerId
            );

            return;
        }

        if (!isConfiguredPeer(peerId)) {

            log.warn(() ->
                    "[TCPHandshakeManager][resolveHandshakeRequest] "
                            + "Unknown peer rejected"
                            + " | peerId=" + peerId
            );

            sendHandshakeResponse(
                    connection,
                    false,
                    peerId
            );

            return;
        }

        /*
         * Register before sending Accepted.
         *
         * The remote peer might send a request
         * immediately after receiving acceptance.
         */
        if (!registerActivePeer(
                peerId,
                connection
        )) {

            log.warn(() ->
                    "[TCPHandshakeManager][resolveHandshakeRequest] "
                            + "Duplicate active connection rejected"
                            + " | peerId=" + peerId
            );

            sendHandshakeResponse(
                    connection,
                    false,
                    peerId
            );

            return;
        }

        sendHandshakeResponse(
                connection,
                true,
                peerId
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

        Message response =
                createHandshakeMessage(
                        MessageType.HANDSHAKE_RESPONSE,
                        accepted
                                ? HANDSHAKE_ACCEPTED
                                : HANDSHAKE_REJECTED
                );

        log.debug(() ->
                "[TCPHandshakeManager][sendHandshakeResponse] "
                        + "Sending handshake response"
                        + " | peerId=" + peerId
                        + " | accepted=" + accepted
        );

        try {

            CompletionStage<Void> stage =
                    Objects.requireNonNull(
                            messageSender.apply(
                                    response,
                                    connection
                            ),
                            "Handshake sender returned null"
                    );

            stage.whenComplete((ignored, exception) -> {

                if (exception != null) {

                    if (accepted && peerId != null) {

                        activePeerConnections.remove(
                                peerId,
                                connection
                        );
                    }

                    log.error(
                            "[TCPHandshakeManager][sendHandshakeResponse] "
                                    + "Handshake response delivery failed"
                                    + " | peerId=" + peerId,
                            exception
                    );

                    closeConnection(connection);
                    return;
                }

                if (accepted) {

                    log.info(() ->
                            "[TCPHandshakeManager][sendHandshakeResponse] "
                                    + "Handshake accepted"
                                    + " | peerId=" + peerId
                    );

                } else {

                    log.warn(() ->
                            "[TCPHandshakeManager][sendHandshakeResponse] "
                                    + "Handshake rejected"
                                    + " | peerId=" + peerId
                    );

                    closeConnection(connection);
                }
            });

        } catch (RuntimeException exception) {

            if (accepted && peerId != null) {

                activePeerConnections.remove(
                        peerId,
                        connection
                );
            }

            log.error(
                    "[TCPHandshakeManager][sendHandshakeResponse] "
                            + "Unable to send handshake response"
                            + " | peerId=" + peerId,
                    exception
            );

            closeConnection(connection);
        }
    }

    // ---------------------------------------------------------
    // OUTGOING HANDSHAKE RESPONSE
    // ---------------------------------------------------------

    private void resolveHandshakeResponse(
            Message handshake,
            ClientConnection connection
    ) {

        String peerId = normalizePeerId(
                handshake.getProcessName()
        );

        if (peerId == null) {

            log.warn(
                    "[TCPHandshakeManager][resolveHandshakeResponse] "
                            + "Handshake response has no peer ID"
            );

            closeConnection(connection);
            return;
        }

        PendingHandshake pending =
                pendingPeerConnections.get(peerId);

        /*
         * A response may only complete a pending
         * handshake on its original connection.
         */
        if (pending == null ||
                pending.connection() != connection) {

            log.warn(() ->
                    "[TCPHandshakeManager][resolveHandshakeResponse] "
                            + "Unexpected handshake response"
                            + " | peerId=" + peerId
            );

            closeConnection(connection);
            return;
        }

        if (handshake.getOperation() != Operation.HANDSHAKE) {

            log.warn(() ->
                    "[TCPHandshakeManager][resolveHandshakeResponse] "
                            + "Invalid handshake operation"
                            + " | peerId=" + peerId
            );

            rejectPendingHandshake(peerId, connection);
            return;
        }

        String responseBody;

        try {

            HandshakePayload payload =
                    messageParser.fromPayload(
                            handshake.getPayload(),
                            HandshakePayload.class
                    );

            responseBody = payload.body();

        } catch (JsonProcessingException exception) {

            log.error(
                    "[TCPHandshakeManager][resolveHandshakeResponse] "
                            + "Unable to decode handshake payload"
                            + " | peerId=" + peerId,
                    exception
            );

            rejectPendingHandshake(peerId, connection);
            return;
        }

        if (!HANDSHAKE_ACCEPTED.equals(responseBody)) {

            log.warn(() ->
                    "[TCPHandshakeManager][resolveHandshakeResponse] "
                            + "Peer rejected handshake"
                            + " | peerId=" + peerId
                            + " | response=" + responseBody
            );

            rejectPendingHandshake(peerId, connection);
            return;
        }

        if (!registerActivePeer(peerId, connection)) {

            log.warn(() ->
                    "[TCPHandshakeManager][resolveHandshakeResponse] "
                            + "Unable to register accepted peer"
                            + " | peerId=" + peerId
            );

            rejectPendingHandshake(peerId, connection);
            return;
        }

        pendingPeerConnections.remove(
                peerId,
                pending
        );

        log.info(() ->
                "[TCPHandshakeManager][resolveHandshakeResponse] "
                        + "Peer handshake completed"
                        + " | peerId=" + peerId
                        + " | activePeers="
                        + activePeerConnections.size()
                        + " | pendingHandshakes="
                        + pendingPeerConnections.size()
        );
    }

    private void rejectPendingHandshake(
            String peerId,
            ClientConnection connection
    ) {

        PendingHandshake pending =
                pendingPeerConnections.get(peerId);

        if (pending != null &&
                pending.connection() == connection) {

            pendingPeerConnections.remove(
                    peerId,
                    pending
            );
        }

        closeConnection(connection);
    }

    // ---------------------------------------------------------
    // REMOVE PEER CONNECTION
    // ---------------------------------------------------------

    public void removePeerConnection(
            String peerId,
            ClientConnection connection
    ) {

        String normalized = normalizePeerId(peerId);

        if (normalized == null || connection == null) {
            return;
        }

        activePeerConnections.remove(
                normalized,
                connection
        );

        PendingHandshake pending =
                pendingPeerConnections.get(normalized);

        if (pending != null &&
                pending.connection() == connection) {

            pendingPeerConnections.remove(
                    normalized,
                    pending
            );
        }

        log.debug(() ->
                "[TCPHandshakeManager][removePeerConnection] "
                        + "Peer connection removed"
                        + " | peerId=" + normalized
        );
    }

    // ---------------------------------------------------------
    // DISCONNECT PEER
    // ---------------------------------------------------------

    public void disconnect(
            String peerId
    ) {

        String normalized = normalizePeerId(peerId);

        if (normalized == null) {
            return;
        }

        ClientConnection active =
                activePeerConnections.remove(normalized);

        PendingHandshake pending =
                pendingPeerConnections.remove(normalized);

        if (active != null) {
            closeConnection(active);
        }

        if (pending != null &&
                pending.connection() != active) {

            closeConnection(pending.connection());
        }

        log.info(() ->
                "[TCPHandshakeManager][disconnect] "
                        + "Peer disconnected"
                        + " | peerId=" + normalized
                        + " | removedActive=" + (active != null)
                        + " | removedPending=" + (pending != null)
        );
    }

    // ---------------------------------------------------------
    // EXPIRED HANDSHAKE CLEANUP
    // ---------------------------------------------------------

    public void cleanupExpiredHandshakes() {

        long now = System.nanoTime();
        int expiredCount = 0;

        for (Map.Entry<String, PendingHandshake> entry :
                pendingPeerConnections.entrySet()) {

            String peerId = entry.getKey();
            PendingHandshake pending = entry.getValue();

            if (now - pending.initiatedAtNanos()
                    < HANDSHAKE_TIMEOUT_NANOS) {
                continue;
            }

            if (pendingPeerConnections.remove(
                    peerId,
                    pending
            )) {

                expiredCount++;

                log.warn(() ->
                        "[TCPHandshakeManager][cleanupExpiredHandshakes] "
                                + "Handshake timed out"
                                + " | peerId=" + peerId
                                + " | timeoutMillis="
                                + HANDSHAKE_TIMEOUT_MILLIS
                );

                closeConnection(pending.connection());
            }
        }

        final int removed = expiredCount;

        log.debug(() ->
                "[TCPHandshakeManager][cleanupExpiredHandshakes] "
                        + "Handshake cleanup completed"
                        + " | removed=" + removed
                        + " | remaining="
                        + pendingPeerConnections.size()
        );
    }

    // ---------------------------------------------------------
    // CONNECTION CLOSED
    // ---------------------------------------------------------

    public void onConnectionClosed(
            ClientConnection closedConnection
    ) {

        if (closedConnection == null) {
            return;
        }

        activePeerConnections.forEach((peerId, connection) -> {

            if (connection == closedConnection &&
                    activePeerConnections.remove(
                            peerId,
                            connection
                    )) {

                log.info(() ->
                        "[TCPHandshakeManager][onConnectionClosed] "
                                + "Disconnected active peer removed"
                                + " | peerId=" + peerId
                );
            }
        });

        pendingPeerConnections.forEach((peerId, pending) -> {

            if (pending.connection() == closedConnection &&
                    pendingPeerConnections.remove(
                            peerId,
                            pending
                    )) {

                log.debug(() ->
                        "[TCPHandshakeManager][onConnectionClosed] "
                                + "Pending handshake removed"
                                + " | peerId=" + peerId
                );
            }
        });
    }

    // ---------------------------------------------------------
    // SHUTDOWN
    // ---------------------------------------------------------

    public void shutdown() {

        log.info(() ->
                "[TCPHandshakeManager][shutdown] "
                        + "Handshake manager shutdown initiated"
                        + " | activePeers="
                        + activePeerConnections.size()
                        + " | pendingHandshakes="
                        + pendingPeerConnections.size()
        );

        for (ClientConnection connection :
                activePeerConnections.values()) {

            closeConnection(connection);
        }

        activePeerConnections.clear();

        for (PendingHandshake pending :
                pendingPeerConnections.values()) {

            closeConnection(pending.connection());
        }

        pendingPeerConnections.clear();

        log.info(
                "[TCPHandshakeManager][shutdown] "
                        + "Handshake manager shutdown completed"
        );
    }
}
