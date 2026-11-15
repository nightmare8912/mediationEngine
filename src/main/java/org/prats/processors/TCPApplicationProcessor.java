
package org.prats.processors;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;
import org.prats.definitions.classes.ProcessContext;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.enums.TCPConnectionMode;
import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.definitions.interfaces.TCPProcessor;
import org.prats.definitions.records.CommandResponse;
import org.prats.definitions.records.TCPPeer;
import org.prats.loggers.EngineLogger;
import org.prats.managers.TCPHandshakeManager;
import org.prats.parsers.MessageParser;
import org.prats.replyChannels.TCPReplyChannel;
import org.prats.services.messageService.MessageService;
import org.prats.services.tcpService.TCPService;

import java.net.Socket;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class TCPApplicationProcessor implements TCPProcessor {

    private static final EngineLogger log =
            EngineLogger.getLogger(TCPApplicationProcessor.class);

    // ---------------------------------------------------------
    // DEPENDENCIES
    // ---------------------------------------------------------

    private final MessageDispatcher messageDispatcher;

    private final MessageParser messageParser;

    private volatile TCPService tcpService;

    // ---------------------------------------------------------
    // CONNECTION CONFIGURATION
    // ---------------------------------------------------------

    private final TCPConnectionMode connectionMode =
            TCPConnectionMode.SERVER_AND_PEERS;

    private final List<TCPPeer> configuredApplicationPeers;

    // ---------------------------------------------------------
    // HANDSHAKE AND PEER REGISTRATION
    // ---------------------------------------------------------

    /*
     * The handshake manager owns the active peer registry.
     *
     * IMPORTANT:
     *
     * This registry is ONLY for registered application peers
     * such as EC01, EC02, etc.
     *
     * An ordinary incoming TCP connection does not need
     * to be registered as an application peer.
     *
     * Shell clients and normal application clients can
     * continue submitting requests without a handshake.
     */
    private final TCPHandshakeManager handshakeManager;

    // ---------------------------------------------------------
    // CONSTRUCTOR
    // ---------------------------------------------------------

    public TCPApplicationProcessor(
            List<TCPPeer> configuredPeers,
            MessageDispatcher messageDispatcher,
            MessageParser messageParser
    ) {

        this.configuredApplicationPeers = List.copyOf(
                Objects.requireNonNull(
                        configuredPeers,
                        "Configured application peers cannot be null"
                )
        );

        this.messageDispatcher = Objects.requireNonNull(
                messageDispatcher,
                "Message dispatcher cannot be null"
        );

        this.messageParser = Objects.requireNonNull(
                messageParser,
                "Message parser cannot be null"
        );

        /*
         * Configure handshake management.
         *
         * The manager will:
         *
         * 1. Initiate handshakes for outgoing EC connections.
         * 2. Process incoming handshake requests.
         * 3. Process incoming handshake responses.
         * 4. Register successfully handshaken peers.
         * 5. Track pending handshakes.
         * 6. Remove expired or disconnected peers.
         *
         * It does NOT prevent this processor from
         * accepting ordinary application clients.
         */
        this.handshakeManager =
                new TCPHandshakeManager(
                        this.configuredApplicationPeers,
                        this.messageParser,
                        this::writeHandshakeMessage
                );

        log.info(() ->
                "[TCPApplicationProcessor][constructor] "
                        + "TCP application processor initialized"
                        + " | connectionMode=" + connectionMode
                        + " | configuredPeers="
                        + configuredApplicationPeers.size()
                        + " | messageDispatcher="
                        + messageDispatcher.getClass().getSimpleName()
                        + " | handshakeManagerEnabled=true"
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
                "[TCPApplicationProcessor][setTcpService] "
                        + "TCP service configured"
                        + " | serviceType="
                        + tcpService.getClass().getSimpleName()
        );
    }

    // ---------------------------------------------------------
    // CONNECTED PEER SOCKETS
    // ---------------------------------------------------------

    @Override
    public List<Socket> connectedSockets() {

        /*
         * Return only registered EC peer sockets.
         *
         * Ordinary shell/client sockets are managed
         * by TCPService, not the active peer registry.
         */
        return handshakeManager.connectedSockets();
    }

    // ---------------------------------------------------------
    // PEER REGISTRATION
    // ---------------------------------------------------------

    @Override
    public boolean isPeerRegistered(
            String peerId
    ) {

        return handshakeManager.isPeerRegistered(peerId);
    }

    public boolean isPeerConnected(
            String peerId
    ) {

        return handshakeManager.isPeerConnected(peerId);
    }

    // ---------------------------------------------------------
    // INCOMING MESSAGE PROCESSING
    // ---------------------------------------------------------

    @Override
    public void processIncoming(
            String rawMessage,
            ClientConnection clientConnection
    ) {

        log.debug(() ->
                "[TCPApplicationProcessor][processIncoming] "
                        + "Incoming TCP message received"
                        + " | messageLength="
                        + (rawMessage == null ? 0 : rawMessage.length())
                        + " | clientConnectionPresent="
                        + (clientConnection != null)
                        + " | processName="
                        + ProcessContext.getProcessName()
        );

        try {

            Objects.requireNonNull(
                    clientConnection,
                    "Client connection cannot be null"
            );

            // ---------------------------------------------
            // PARSE INCOMING MESSAGE
            // ---------------------------------------------

            /*
             * Preserve the existing MessageParser behavior.
             *
             * JSON messages are parsed normally.
             *
             * Plain-text shell commands are converted
             * into your existing command message format
             * by the MessageParser.
             */
            Message incomingMessage =
                    Objects.requireNonNull(
                            messageParser.createMessageFromJSON(
                                    rawMessage
                            ),
                            "Parsed application message cannot be null"
                    );

            log.debug(() ->
                    "[TCPApplicationProcessor][processIncoming] "
                            + "Initial message parsing completed"
                            + " | operation="
                            + incomingMessage.getOperation()
                            + " | messageType="
                            + incomingMessage.getMessageType()
                            + " | correlationId="
                            + incomingMessage.getCorrelationId()
                            + " | sender="
                            + incomingMessage.getProcessName()
            );

            // ---------------------------------------------
            // HANDSHAKE PROCESSING
            // ---------------------------------------------

            /*
             * Check whether this is an EC handshake message.
             *
             * Only HANDSHAKE_REQUEST and HANDSHAKE_RESPONSE
             * are handled by TCPHandshakeManager.
             *
             * If this is a handshake message, the manager
             * processes it and we return immediately.
             *
             * Normal shell/application messages skip
             * this branch and continue to MessageService.
             */
            if (handshakeManager.processHandshake(
                    incomingMessage,
                    clientConnection
            )) {

                log.debug(() ->
                        "[TCPApplicationProcessor][processIncoming] "
                                + "Application peer handshake processed"
                                + " | messageType="
                                + incomingMessage.getMessageType()
                                + " | peerId="
                                + incomingMessage.getProcessName()
                                + " | activePeers="
                                + handshakeManager.getActivePeerCount()
                                + " | pendingHandshakes="
                                + handshakeManager.getPendingHandshakeCount()
                );

                return;
            }

            // ---------------------------------------------
            // ORDINARY APPLICATION PROCESSING
            // ---------------------------------------------

            /*
             * IMPORTANT:
             *
             * We DO NOT require a registered peer here.
             *
             * Shell clients, external application clients,
             * and other permitted incoming requests
             * continue through the original processing path.
             *
             * Being unregistered as an EC peer must not
             * cause the application processor to close
             * an ordinary client's socket.
             *
             * Authorization for sensitive operations
             * remains the responsibility of the
             * appropriate application/security handler.
             */

            log.debug(() ->
                    "[TCPApplicationProcessor][processIncoming] "
                            + "Preparing request dispatch"
                            + " | correlationId="
                            + incomingMessage.getCorrelationId()
                            + " | operation="
                            + incomingMessage.getOperation()
                            + " | messageType="
                            + incomingMessage.getMessageType()
            );

            /*
             * Dispatch the request to the internal message system.
             *
             * A TCPReplyChannel is provided so downstream services
             * can send their responses to the original client.
             */
            TCPReplyChannel replyChannel =
                    new TCPReplyChannel(
                            this,
                            clientConnection
                    );

            log.debug(() ->
                    "[TCPApplicationProcessor][processIncoming] "
                            + "TCP reply channel initialized"
                            + " | correlationId="
                            + incomingMessage.getCorrelationId()
            );

            messageDispatcher.dispatchRequest(
                    incomingMessage,
                    replyChannel
            );

            log.debug(() ->
                    "[TCPApplicationProcessor][processIncoming] "
                            + "Request dispatched successfully"
                            + " | correlationId="
                            + incomingMessage.getCorrelationId()
                            + " | operation="
                            + incomingMessage.getOperation()
            );

        } catch (RuntimeException exception) {

            /*
             * Log the failure without exposing the raw message,
             * account credentials, or client session token.
             *
             * Rethrow so the existing upstream error-handling
             * mechanism remains responsible for recovery.
             */
            log.error(
                    "[TCPApplicationProcessor][processIncoming] "
                            + "Incoming TCP message processing failed"
                            + " | exception="
                            + exception.getClass().getSimpleName(),
                    exception
            );

            throw exception;
        }
    }

    // ---------------------------------------------------------
    // DIRECT HANDSHAKE MESSAGE WRITE
    // ---------------------------------------------------------

    /*
     * Handshake messages must be sent over the exact
     * socket being established.
     *
     * We cannot use a registered-peer lookup here
     * because the handshake has not completed yet.
     *
     * This method is supplied to TCPHandshakeManager
     * as its message-sending callback.
     */
    private CompletionStage<Void> writeHandshakeMessage(
            Message message,
            ClientConnection connection
    ) {

        Objects.requireNonNull(
                message,
                "Handshake message cannot be null"
        );

        Objects.requireNonNull(
                connection,
                "Handshake connection cannot be null"
        );

        TCPService service = tcpService;

        if (service == null) {

            log.error(
                    "[TCPApplicationProcessor][writeHandshakeMessage] "
                            + "TCP service has not been initialized"
            );

            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "TCP service has not been initialized"
                    )
            );
        }

        log.debug(() ->
                "[TCPApplicationProcessor][writeHandshakeMessage] "
                        + "Sending application handshake message"
                        + " | messageType="
                        + message.getMessageType()
                        + " | operation="
                        + message.getOperation()
                        + " | correlationId="
                        + message.getCorrelationId()
        );

        try {

            /*
             * Handshake messages must retain their
             * complete JSON envelope.
             */
            CompletionStage<Void> writeStage =
                    service.writeMessage(
                            serialize(message),
                            connection
                    );

            return writeStage.whenComplete(
                    (ignored, exception) -> {

                        if (exception != null) {

                            log.error(
                                    "[TCPApplicationProcessor][writeHandshakeMessage] "
                                            + "Handshake message delivery failed"
                                            + " | correlationId="
                                            + message.getCorrelationId(),
                                    exception
                            );

                        } else {

                            log.debug(() ->
                                    "[TCPApplicationProcessor][writeHandshakeMessage] "
                                            + "Handshake message submitted"
                                            + " | messageType="
                                            + message.getMessageType()
                            );
                        }
                    }
            );

        } catch (RuntimeException exception) {

            log.error(
                    "[TCPApplicationProcessor][writeHandshakeMessage] "
                            + "Unable to submit handshake message",
                    exception
            );

            return CompletableFuture.failedFuture(exception);
        }
    }

    // ---------------------------------------------------------
    // CONNECTION CLOSED
    // ---------------------------------------------------------

    @Override
    public void onConnectionClosed(
            ClientConnection connection
    ) {

        log.debug(
                "[TCPApplicationProcessor][onConnectionClosed] "
                        + "Connection closure notification received"
        );

        /*
         * Remove any registered EC peer or pending
         * handshake associated with this connection.
         *
         * Ordinary client connections will not have
         * entries in the handshake manager.
         */
        handshakeManager.onConnectionClosed(
                connection
        );

        /*
         * Preserve existing MessageService connection
         * release behavior.
         */
        if (messageDispatcher instanceof MessageService service) {

            service.releaseConnection(connection);
        }

        TCPProcessor.super.onConnectionClosed(connection);
    }

    // ---------------------------------------------------------
    // INTERNAL REQUEST DISPATCH
    // ---------------------------------------------------------

    private void dispatchIncoming(
            Message request,
            ClientConnection connection
    ) {

        messageDispatcher.dispatchRequest(
                request,
                new TCPReplyChannel(this, connection)
        );
    }

    // ---------------------------------------------------------
    // OUTGOING APPLICATION MESSAGE PROCESSING
    // ---------------------------------------------------------

    public CompletionStage<Void> processOutgoing(
            Message outgoing,
            ClientConnection clientConnection
    ) {

        Objects.requireNonNull(
                outgoing,
                "Outgoing message cannot be null"
        );

        Objects.requireNonNull(
                clientConnection,
                "Client connection cannot be null"
        );

        log.debug(() ->
                "[TCPApplicationProcessor][processOutgoing] "
                        + "Preparing outgoing TCP message"
                        + " | operation="
                        + outgoing.getOperation()
                        + " | responseComplete="
                        + outgoing.isResponseComplete()
                        + " | clientConnectionPresent=true"
        );

        /*
         * Preserve the original outgoing response behavior.
         *
         * We do not require handshake registration here
         * because ordinary clients must still receive
         * responses over their existing TCP connection.
         */

        // ---------------------------------------------
        // COMMAND RESPONSE PROCESSING
        // ---------------------------------------------

        if (outgoing.getOperation() == Operation.COMMAND) {

            log.debug(
                    "[TCPApplicationProcessor][processOutgoing] "
                            + "COMMAND operation detected; extracting response body"
            );

            try {

                String commandBody =
                        outgoing.hasError()
                                ? outgoing.getError().getErrorMessage()
                                : messageParser.fromPayload(
                                outgoing.getPayload(),
                                CommandResponse.class
                        ).body();

                log.debug(() ->
                        "[TCPApplicationProcessor][processOutgoing] "
                                + "COMMAND body extracted"
                                + " | bodyLength="
                                + (commandBody == null
                                ? 0
                                : commandBody.length())
                );

                return tcpService.writeMessage(
                        commandBody,
                        clientConnection
                );

            } catch (JsonProcessingException exception) {

                log.error(
                        "[TCPApplicationProcessor][processOutgoing] "
                                + "Unable to extract COMMAND response body",
                        exception
                );
            }
        }

        // ---------------------------------------------
        // NORMAL APPLICATION MESSAGE
        // ---------------------------------------------

        log.debug(() ->
                "[TCPApplicationProcessor][processOutgoing] "
                        + "Serializing outgoing message"
                        + " | operation="
                        + outgoing.getOperation()
        );

        return tcpService.writeMessage(
                serialize(outgoing),
                clientConnection
        );
    }

    // ---------------------------------------------------------
    // PROCESSOR SHUTDOWN
    // ---------------------------------------------------------

    @Override
    public void informShutdown() {

        log.info(() ->
                "[TCPApplicationProcessor][informShutdown] "
                        + "Application processor shutdown initiated"
                        + " | activePeers="
                        + handshakeManager.getActivePeerCount()
                        + " | pendingHandshakes="
                        + handshakeManager.getPendingHandshakeCount()
        );

        /*
         * Close registered EC peer connections and
         * incomplete handshake connections.
         *
         * Ordinary incoming clients remain owned
         * by TCPService's connection lifecycle.
         */
        handshakeManager.shutdown();

        log.info(
                "[TCPApplicationProcessor][informShutdown] "
                        + "Application processor shutdown completed"
        );
    }

    // ---------------------------------------------------------
    // HANDSHAKE CLEANUP
    // ---------------------------------------------------------

    @Override
    public void cleanupCorrelations() {

        /*
         * The original application processor did not
         * own a TCP correlation registry.
         *
         * However, now that it initiates EC handshakes,
         * expired pending handshakes must be removed.
         */
        log.debug(
                "[TCPApplicationProcessor][cleanupCorrelations] "
                        + "Application handshake cleanup started"
        );

        handshakeManager.cleanupExpiredHandshakes();

        log.debug(
                "[TCPApplicationProcessor][cleanupCorrelations] "
                        + "Application handshake cleanup completed"
        );
    }

    // ---------------------------------------------------------
    // PEER ID NORMALIZATION
    // ---------------------------------------------------------

    private static String normalizePeerId(
            String peerId
    ) {

        return TCPHandshakeManager.normalizePeerId(
                peerId
        );
    }

    // ---------------------------------------------------------
    // CONNECTION OWNERSHIP
    // ---------------------------------------------------------

    private boolean shouldInitiateConnection(
            String localProcessId,
            String remoteProcessId
    ) {

        String local =
                normalizePeerId(localProcessId);

        String remote =
                normalizePeerId(remoteProcessId);

        if (local == null || remote == null) {

            log.warn(
                    "[TCPApplicationProcessor][shouldInitiateConnection] "
                            + "Cannot evaluate connection ownership because process ID is missing"
            );

            return false;
        }

        /*
         * Preserve the original deterministic rule.
         *
         * Only one of the ECs initiates the application
         * TCP connection.
         *
         * Example:
         *
         * EC01 < EC02
         *
         * EC01 initiates the connection to EC02.
         * EC02 accepts the connection.
         *
         * Both sides then register their peer
         * through the handshake protocol.
         */
        boolean shouldInitiate =
                local.compareTo(remote) < 0;

        log.debug(() ->
                "[TCPApplicationProcessor][shouldInitiateConnection] "
                        + "Peer connection ownership evaluated"
                        + " | localProcessId=" + local
                        + " | remoteProcessId=" + remote
                        + " | shouldInitiate=" + shouldInitiate
        );

        return shouldInitiate;
    }

    // ---------------------------------------------------------
    // PEER RECONCILIATION
    // ---------------------------------------------------------

    @Override
    public synchronized void reconcilePeers() {

        log.debug(() ->
                "[TCPApplicationProcessor][reconcilePeers] "
                        + "Starting peer reconciliation"
                        + " | connectionMode=" + connectionMode
                        + " | configuredPeers="
                        + configuredApplicationPeers.size()
                        + " | activePeers="
                        + handshakeManager.getActivePeerCount()
                        + " | pendingHandshakes="
                        + handshakeManager.getPendingHandshakeCount()
        );

        if (connectionMode == TCPConnectionMode.SERVER_ONLY) {

            log.debug(
                    "[TCPApplicationProcessor][reconcilePeers] "
                            + "Peer reconciliation skipped because connection mode is SERVER_ONLY"
            );

            return;
        }

        TCPService service = tcpService;

        if (service == null) {

            log.error(
                    "[TCPApplicationProcessor][reconcilePeers] "
                            + "TCP service has not been initialized"
            );

            return;
        }

        /*
         * Cleanup incomplete outgoing handshakes
         * before attempting further connections.
         */
        handshakeManager.cleanupExpiredHandshakes();

        // ---------------------------------------------
        // RECONCILE CONFIGURED EC PEERS
        // ---------------------------------------------

        for (TCPPeer tcpPeer : configuredApplicationPeers) {

            if (tcpPeer == null) {

                log.debug(
                        "[TCPApplicationProcessor][reconcilePeers] "
                                + "Skipping null peer configuration"
                );

                continue;
            }

            String peerId =
                    normalizePeerId(
                            tcpPeer.processId()
                    );

            if (peerId == null) {

                log.warn(
                        "[TCPApplicationProcessor][reconcilePeers] "
                                + "Skipping peer with invalid process ID"
                );

                continue;
            }

            log.debug(() ->
                    "[TCPApplicationProcessor][reconcilePeers] "
                            + "Evaluating configured peer"
                            + " | peerProcessId=" + peerId
                            + " | peerHost="
                            + tcpPeer.host()
                            + " | peerPort="
                            + tcpPeer.port()
            );

            // ---------------------------------------------
            // CONNECTION INITIATION OWNERSHIP
            // ---------------------------------------------

            if (!shouldInitiateConnection(
                    ProcessContext.getProcessName(),
                    peerId
            )) {

                log.debug(() ->
                        "[TCPApplicationProcessor][reconcilePeers] "
                                + "Skipping peer because this process does not own connection initiation"
                                + " | localProcessId="
                                + ProcessContext.getProcessName()
                                + " | peerProcessId=" + peerId
                );

                continue;
            }

            // ---------------------------------------------
            // CHECK ACTIVE PEER CONNECTION
            // ---------------------------------------------

            /*
             * A peer is active only after the handshake
             * manager registers its connection.
             *
             * TCP connection establishment alone
             * does not populate the active peer registry.
             */
            ClientConnection existingConnection =
                    handshakeManager.getActiveConnection(
                            peerId
                    );

            if (TCPHandshakeManager.isConnectionUsable(
                    existingConnection
            )) {

                log.debug(() ->
                        "[TCPApplicationProcessor][reconcilePeers] "
                                + "Peer already has a usable registered connection"
                                + " | peerProcessId=" + peerId
                );

                continue;
            }

            // ---------------------------------------------
            // REMOVE STALE REGISTERED PEER
            // ---------------------------------------------

            if (existingConnection != null) {

                log.warn(() ->
                        "[TCPApplicationProcessor][reconcilePeers] "
                                + "Removing stale registered peer connection"
                                + " | peerProcessId=" + peerId
                );

                handshakeManager.disconnect(peerId);
            }

            // ---------------------------------------------
            // CHECK PENDING HANDSHAKE
            // ---------------------------------------------

            /*
             * Do not open another connection while
             * the previous connection is waiting
             * for its handshake response.
             */
            if (handshakeManager.hasPendingHandshake(peerId)) {

                log.debug(() ->
                        "[TCPApplicationProcessor][reconcilePeers] "
                                + "Skipping peer because handshake is pending"
                                + " | peerProcessId=" + peerId
                );

                continue;
            }

            // ---------------------------------------------
            // ESTABLISH TCP CONNECTION
            // ---------------------------------------------

            log.debug(() ->
                    "[TCPApplicationProcessor][reconcilePeers] "
                            + "Attempting application peer connection"
                            + " | peerProcessId=" + peerId
                            + " | host=" + tcpPeer.host()
                            + " | port=" + tcpPeer.port()
            );

            ClientConnection connection;

            try {

                connection =
                        service.attemptConnection(
                                tcpPeer
                        );

            } catch (Exception exception) {

                log.error(
                        "[TCPApplicationProcessor][reconcilePeers] "
                                + "Peer connection attempt failed"
                                + " | peerProcessId=" + peerId,
                        exception
                );

                continue;
            }

            if (connection == null) {

                log.warn(() ->
                        "[TCPApplicationProcessor][reconcilePeers] "
                                + "Peer connection attempt failed; retry during next reconciliation"
                                + " | peerProcessId=" + peerId
                                + " | host=" + tcpPeer.host()
                                + " | port=" + tcpPeer.port()
                );

                continue;
            }

            // ---------------------------------------------
            // INITIATE EC HANDSHAKE
            // ---------------------------------------------

            /*
             * IMPORTANT:
             *
             * Do not insert this connection directly
             * into the active peer registry.
             *
             * TCPHandshakeManager will register it
             * after receiving Accepted from the
             * expected peer on the same connection.
             *
             * Note that initiateHandshake() completes
             * when the handshake REQUEST is submitted.
             * It does not wait for handshake acceptance.
             */
            handshakeManager.initiateHandshake(
                    peerId,
                    connection
            ).whenComplete((ignored, exception) -> {

                if (exception != null) {

                    log.error(
                            "[TCPApplicationProcessor][reconcilePeers] "
                                    + "Unable to initiate application peer handshake"
                                    + " | peerProcessId=" + peerId,
                            exception
                    );

                } else {

                    log.debug(() ->
                            "[TCPApplicationProcessor][reconcilePeers] "
                                    + "Handshake request submitted; awaiting peer response"
                                    + " | peerProcessId=" + peerId
                                    + " | pendingHandshakes="
                                    + handshakeManager.getPendingHandshakeCount()
                    );
                }
            });
        }

        log.debug(() ->
                "[TCPApplicationProcessor][reconcilePeers] "
                        + "Peer reconciliation completed"
                        + " | activePeers="
                        + handshakeManager.getActivePeerCount()
                        + " | pendingHandshakes="
                        + handshakeManager.getPendingHandshakeCount()
        );
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
                        Objects.requireNonNull(
                                message,
                                "Message cannot be null"
                        )
                );

        log.debug(() ->
                "[TCPApplicationProcessor][serialize] "
                        + "Message serialized"
                        + " | operation=" + message.getOperation()
                        + " | messageType=" + message.getMessageType()
                        + " | serializedLength="
                        + (serializedMessage == null
                        ? 0
                        : serializedMessage.length())
        );

        return serializedMessage;
    }
}
