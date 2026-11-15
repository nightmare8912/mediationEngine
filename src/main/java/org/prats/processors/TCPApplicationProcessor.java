package org.prats.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;
import org.prats.definitions.classes.ProcessContext;
import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.enums.TCPConnectionMode;
import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.definitions.interfaces.TCPProcessor;
import org.prats.definitions.records.TCPPeer;
import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;
import org.prats.replyChannels.TCPReplyChannel;
import org.prats.services.tcpService.TCPService;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

public class TCPApplicationProcessor implements TCPProcessor {

    private static final EngineLogger log =
            EngineLogger.getLogger(TCPApplicationProcessor.class);

    private final MessageDispatcher messageDispatcher;
    private final MessageParser messageParser;

    private final TCPConnectionMode connectionMode
            = TCPConnectionMode.SERVER_AND_PEERS;

    private final List<TCPPeer> configuredApplicationPeers;

    private final Map<String, ClientConnection> activePeerConnections =
            new ConcurrentHashMap<>();

    private TCPService tcpService;
    private org.prats.security.UserAccountService accounts;

    @Override
    public List<Socket> connectedSockets() {

        return activePeerConnections.values().stream().
                map(ClientConnection::getSocket)
                .filter(socket -> !socket.isClosed())
                .toList();

    }

    public void setUserAccountService(org.prats.security.UserAccountService accounts) { this.accounts = accounts; }

    private Message accountCommand(String raw) {

        log.debug(() ->
                "[CommandProcessor][accountCommand] Checking incoming command"
                        + " | commandPresent=" + (raw != null)
        );

        String[] parts = raw.trim().split("\\s+", 4);

        /*
         * Validate that the command belongs to the account
         * management category and uses a supported action.
         */
        if (parts.length < 2
                || !parts[0].equalsIgnoreCase("mng")
                || !Set.of(
                "register",
                "login",
                "logout",
                "whoami"
        ).contains(parts[1].toLowerCase(Locale.ROOT))) {

            log.debug(() ->
                    "[CommandProcessor][accountCommand] Command not recognized as an account command"
            );

            return null;
        }

        /*
         * Normalize the action using a locale-independent
         * lowercase conversion.
         */
        String action = parts[1].toLowerCase(Locale.ROOT);

        log.debug(() ->
                "[CommandProcessor][accountCommand] Account command recognized"
                        + " | action=" + action
                        + " | argumentCount=" + Math.max(0, parts.length - 2)
        );

        /*
         * Construct the internal command payload.
         *
         * Never log usernames/passwords or the raw
         * command because credentials may be present.
         */
        Map<String, Object> values = new HashMap<>();

        values.put("action", action);
        values.put("textReply", true);

        if (parts.length > 2) {

            values.put("username", parts[2]);

            log.debug(() ->
                    "[CommandProcessor][accountCommand] Username argument provided"
                            + " | action=" + action
            );
        }

        if (parts.length > 3) {

            values.put("password", parts[3]);

            log.debug(() ->
                    "[CommandProcessor][accountCommand] Password argument provided"
                            + " | action=" + action
            );
        }

        /*
         * Convert the parsed command into the internal
         * message structure for GUI request processing.
         */
        log.debug(() ->
                "[CommandProcessor][accountCommand] Creating internal account request"
                        + " | action=" + action
                        + " | operation=GUI"
                        + " | messageType=REQUEST"
        );

        Message message = messageParser.createEmptyMessage();

        message.setMessageType(MessageType.REQUEST);
        message.setOperation(Operation.GUI);
        message.setPayload(
                messageParser.convertToJsonNode(values)
        );

        log.debug(() ->
                "[CommandProcessor][accountCommand] Account request created successfully"
                        + " | action=" + action
                        + " | correlationId=" + message.getCorrelationId()
                        + " | operation=" + message.getOperation()
                        + " | messageType=" + message.getMessageType()
        );

        return message;
    }

    public TCPApplicationProcessor(
            List<TCPPeer> configuredPeers,
            MessageDispatcher messageDispatcher,
            MessageParser messageParser
    ) {
        this.configuredApplicationPeers = configuredPeers;
        this.messageDispatcher = messageDispatcher;
        this.messageParser = messageParser;

        log.info(() ->
                "[TCPApplicationProcessor][constructor] TCP application processor initialized"
                        + " | connectionMode=" + connectionMode
                        + " | configuredPeers="
                        + (configuredPeers == null ? 0 : configuredPeers.size())
                        + " | messageDispatcher="
                        + (messageDispatcher == null
                        ? "null"
                        : messageDispatcher.getClass().getSimpleName())
        );
    }

    public void setTcpService(TCPService tcpService) {
        this.tcpService = tcpService;

        log.debug(() ->
                "[TCPApplicationProcessor][setTcpService] TCP service configured"
                        + " | serviceType="
                        + (tcpService == null
                        ? "null"
                        : tcpService.getClass().getSimpleName())
        );
    }

//    private void registerHandshake(Message handshake, ClientConnection clientConnection) {
//        activePeerConnections.put(
//                handshake.getProcessName(),
//                clientConnection
//        );
//    }

    @Override
    public void processIncoming(
            String rawMessage,
            ClientConnection clientConnection
    ) {

        log.debug(() ->
                "[TCPApplicationProcessor][processIncoming] Incoming TCP message received"
                        + " | messageLength=" + (rawMessage == null ? 0 : rawMessage.length())
                        + " | clientConnectionPresent=" + (clientConnection != null)
                        + " | processName=" + ProcessContext.getProcessName()
        );

        try {

            /*
             * Check whether the incoming message is an account command.
             *
             * Account commands are converted directly into internal
             * GUI request messages. All other messages are passed
             * to the standard JSON message parser.
             */
            log.debug(() ->
                    "[TCPApplicationProcessor][processIncoming] Checking for account command"
                            + " | accountServiceAvailable=" + (accounts != null)
            );

            Message accountRequest =
                    accounts == null
                            ? null
                            : accountCommand(rawMessage);

            log.debug(() ->
                    "[TCPApplicationProcessor][processIncoming] Account command detection completed"
                            + " | accountCommandRecognized=" + (accountRequest != null)
            );

            /*
             * Parse the incoming message into the internal
             * message representation.
             */
            Message incomingMessage =
                    accountRequest != null
                            ? accountRequest
                            : messageParser.createMessageFromJSON(rawMessage);

            log.debug(() ->
                    "[TCPApplicationProcessor][processIncoming] Initial message parsing completed"
                            + " | messageParsed=" + (incomingMessage != null)
                            + " | operation=" + (incomingMessage == null ? null : incomingMessage.getOperation())
                            + " | messageType=" + (incomingMessage == null ? null : incomingMessage.getMessageType())
                            + " | correlationId=" + (incomingMessage == null ? null : incomingMessage.getCorrelationId())
            );

            /*
             * Attach the client session to the incoming message.
             *
             * Downstream handlers can use this session for
             * authorization and account-related operations.
             */
            log.debug(() ->
                    "[TCPApplicationProcessor][processIncoming] Attaching client session"
                            + " | correlationId=" + incomingMessage.getCorrelationId()
                            + " | sessionPresent=" + (clientConnection.getSessionContext() != null)
            );

            incomingMessage.setClientSession(
                    clientConnection.getSessionContext()
            );

            log.debug(() ->
                    "[TCPApplicationProcessor][processIncoming] Client session attached successfully"
                            + " | correlationId=" + incomingMessage.getCorrelationId()
            );

            /*
             * Enforce authentication when the account service
             * is enabled.
             *
             * GUI operations are exempt because they include
             * account registration and login requests.
             */
            if (accounts != null
                    && incomingMessage.getOperation() != Operation.GUI) {

                log.debug(() ->
                        "[TCPApplicationProcessor][processIncoming] Validating client authentication"
                                + " | correlationId=" + incomingMessage.getCorrelationId()
                                + " | operation=" + incomingMessage.getOperation()
                );

                boolean authenticated =
                        accounts.session(
                                clientConnection.getSessionContext().token()
                        ).isPresent();

                if (!authenticated) {

                    log.debug(() ->
                            "[TCPApplicationProcessor][processIncoming] No authenticated session found"
                                    + " | correlationId=" + incomingMessage.getCorrelationId()
                    );

                    /*
                     * Allow unauthenticated clients to execute
                     * the account management help command.
                     */
                    boolean help =
                            rawMessage.trim()
                                    .replaceAll("\\s+", " ")
                                    .equalsIgnoreCase("mng help");

                    if (!help) {

                        log.warn(() ->
                                "[TCPApplicationProcessor][processIncoming] Incoming request rejected"
                                        + " | reason=AUTHENTICATION_REQUIRED"
                                        + " | correlationId=" + incomingMessage.getCorrelationId()
                                        + " | operation=" + incomingMessage.getOperation()
                        );

                        tcpService.writeMessage(
                                "Login required. Use mng register <username> <password> "
                                        + "or mng login <username> <password>.",
                                clientConnection
                        );

                        log.debug(() ->
                                "[TCPApplicationProcessor][processIncoming] Authentication failure response submitted"
                                        + " | correlationId=" + incomingMessage.getCorrelationId()
                        );

                        return;
                    }

                    log.debug(() ->
                            "[TCPApplicationProcessor][processIncoming] Unauthenticated help command permitted"
                                    + " | correlationId=" + incomingMessage.getCorrelationId()
                    );

                } else {

                    log.debug(() ->
                            "[TCPApplicationProcessor][processIncoming] Client authentication successful"
                                    + " | correlationId=" + incomingMessage.getCorrelationId()
                    );
                }

            } else {

                log.debug(() ->
                        "[TCPApplicationProcessor][processIncoming] Authentication validation skipped"
                                + " | correlationId=" + incomingMessage.getCorrelationId()
                                + " | accountServiceAvailable=" + (accounts != null)
                                + " | operation=" + incomingMessage.getOperation()
                );
            }

            /*
             * Dispatch the request to the internal message system.
             *
             * A TCPReplyChannel is provided so downstream services
             * can send their responses to the original client.
             */
            log.debug(() ->
                    "[TCPApplicationProcessor][processIncoming] Preparing request dispatch"
                            + " | correlationId=" + incomingMessage.getCorrelationId()
                            + " | operation=" + incomingMessage.getOperation()
                            + " | messageType=" + incomingMessage.getMessageType()
            );

            TCPReplyChannel replyChannel =
                    new TCPReplyChannel(
                            this,
                            clientConnection
                    );

            log.debug(() ->
                    "[TCPApplicationProcessor][processIncoming] TCP reply channel initialized"
                            + " | correlationId=" + incomingMessage.getCorrelationId()
            );

            messageDispatcher.dispatchRequest(
                    incomingMessage,
                    replyChannel
            );

            log.debug(() ->
                    "[TCPApplicationProcessor][processIncoming] Request dispatched successfully"
                            + " | correlationId=" + incomingMessage.getCorrelationId()
                            + " | operation=" + incomingMessage.getOperation()
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
                    "[TCPApplicationProcessor][processIncoming] Incoming TCP message processing failed"
                            + " | exception=" + exception.getClass().getSimpleName()
            );

            throw exception;
        }
    }

    @Override
    public void onConnectionClosed(ClientConnection connection) {
        if (messageDispatcher instanceof org.prats.services.messageService.MessageService service) service.releaseConnection(connection);
        if (accounts != null) accounts.logout(connection.getSessionContext().token());
        connection.getSessionContext().clear();
        org.prats.definitions.interfaces.TCPProcessor.super.onConnectionClosed(connection);
    }

    public CompletionStage<Void> processOutgoing(
            Message outgoing,
            ClientConnection clientConnection
    ) {

        log.debug(() ->
                "[TCPApplicationProcessor][processOutgoing] Preparing outgoing TCP message"
                        + " | operation=" + outgoing.getOperation()
                        + " | responseComplete=" + outgoing.isResponseComplete()
                        + " | clientConnectionPresent=" + (clientConnection != null)
        );

        if (outgoing.getOperation() == Operation.COMMAND) {

            log.debug(
                    "[TCPApplicationProcessor][processOutgoing] COMMAND operation detected; extracting response body"
            );

            try {
                String commandBody = messageParser.extractPayloadField(
                        outgoing,
                        "body",
                        String.class
                );

                log.debug(() ->
                        "[TCPApplicationProcessor][processOutgoing] COMMAND body extracted"
                                + " | bodyLength="
                                + (commandBody == null ? 0 : commandBody.length())
                );

                return tcpService.writeMessage(
                        commandBody,
                        clientConnection
                );

            } catch (JsonProcessingException jsonProcessingException) {

                log.error(
                        "[TCPApplicationProcessor][processOutgoing] Unable to extract COMMAND response body",
                        jsonProcessingException
                );
            }
        }

        log.debug(() ->
                "[TCPApplicationProcessor][processOutgoing] Serializing outgoing message"
                        + " | operation=" + outgoing.getOperation()
        );

        return tcpService.writeMessage(
                serialize(outgoing),
                clientConnection
        );
    }

    @Override
    public void informShutdown() {
        log.debug(
                "[TCPApplicationProcessor][informShutdown] Shutdown notification received; no action required"
        );
// not required
    }

    @Override
    public void cleanupCorrelations() {
        log.debug(
                "[TCPApplicationProcessor][cleanupCorrelations] Correlation cleanup invoked; no action required"
        );
// not required
    }

    private static String normalizePeerId(String peerId) {

        if (peerId == null || peerId.isBlank()) {
            log.debug(
                    "[TCPApplicationProcessor][normalizePeerId] Empty peer ID supplied"
            );

            return null;
        }

        String normalizedPeerId = peerId.trim().toUpperCase(Locale.ROOT);

        log.debug(() ->
                "[TCPApplicationProcessor][normalizePeerId] Peer ID normalized"
                        + " | normalizedPeerId=" + normalizedPeerId
        );

        return normalizedPeerId;
    }

    private boolean shouldInitiateConnection(
            String localProcessId,
            String remoteProcessId
    ) {

        String local = normalizePeerId(localProcessId);
        String remote = normalizePeerId(remoteProcessId);

        boolean shouldInitiate = local.compareTo(remote) < 0;

        log.debug(() ->
                "[TCPApplicationProcessor][shouldInitiateConnection] Peer connection ownership evaluated"
                        + " | localProcessId=" + local
                        + " | remoteProcessId=" + remote
                        + " | shouldInitiate=" + shouldInitiate
        );

        return shouldInitiate;
    }


    @Override
    public synchronized void reconcilePeers() {

        log.debug(() ->
                "[TCPApplicationProcessor][reconcilePeers] Starting peer reconciliation"
                        + " | connectionMode=" + connectionMode
                        + " | configuredPeers="
                        + (configuredApplicationPeers == null ? 0 : configuredApplicationPeers.size())
                        + " | activePeerConnections=" + activePeerConnections.size()
        );

        if (connectionMode == TCPConnectionMode.SERVER_ONLY) {

            log.debug(
                    "[TCPApplicationProcessor][reconcilePeers] Peer reconciliation skipped because connection mode is SERVER_ONLY"
            );

            return;
        }

        for (TCPPeer tcpPeer : configuredApplicationPeers) {

            if (tcpPeer == null) {
                log.debug(
                        "[TCPApplicationProcessor][reconcilePeers] Skipping null peer configuration"
                );

                continue;
            }

            log.debug(() ->
                    "[TCPApplicationProcessor][reconcilePeers] Evaluating configured peer"
                            + " | peerProcessId=" + tcpPeer.processId()
                            + " | peerHost=" + tcpPeer.host()
                            + " | peerPort=" + tcpPeer.port()
            );

            if (!shouldInitiateConnection(ProcessContext.getProcessName(), tcpPeer.processId())) {

                log.debug(() ->
                        "[TCPApplicationProcessor][reconcilePeers] Skipping peer because this process does not own connection initiation"
                                + " | localProcessId=" + ProcessContext.getProcessName()
                                + " | peerProcessId=" + tcpPeer.processId()
                );

                continue;
            }

            ClientConnection existingConnection =
                    activePeerConnections.get(tcpPeer.processId());

            if (isConnectionUsable(existingConnection)) {

                log.debug(() ->
                        "[TCPApplicationProcessor][reconcilePeers] Peer already has a usable connection"
                                + " | peerProcessId=" + tcpPeer.processId()
                );

                continue;
            }

            if (existingConnection != null) {
                activePeerConnections.remove(
                        tcpPeer.processId(),
                        existingConnection
                );
                try {
                    existingConnection.getSocket().close();
                } catch (IOException exception) {
                    log.warn(() ->
                            "[TCPApplicationProcessor][reconcilePeers] Unable to close stale peer connection"
                                    + " | peerProcessId=" + tcpPeer.processId()
                                    + " | reason=" + exception.getMessage()
                    );
                }
            }

            log.debug(() ->
                    "[TCPApplicationProcessor][reconcilePeers] Attempting peer connection"
                            + " | peerProcessId=" + tcpPeer.processId()
                            + " | host=" + tcpPeer.host()
                            + " | port=" + tcpPeer.port()
            );

            ClientConnection connection = tcpService.attemptConnection(tcpPeer);

            if (connection != null) {

                activePeerConnections.put(
                        tcpPeer.processId(),
                        connection
                );

                log.info(() ->
                        "[TCPApplicationProcessor][reconcilePeers] Peer connection established"
                                + " | peerProcessId=" + tcpPeer.processId()
                                + " | activePeerConnections=" + activePeerConnections.size()
                );

            } else {

                log.warn(() ->
                        "[TCPApplicationProcessor][reconcilePeers] Peer connection attempt failed; it will be retried during the next reconciliation"
                                + " | peerProcessId=" + tcpPeer.processId()
                                + " | host=" + tcpPeer.host()
                                + " | port=" + tcpPeer.port()
                );
            }
        }

        log.debug(() ->
                "[TCPApplicationProcessor][reconcilePeers] Peer reconciliation completed"
                        + " | activePeerConnections=" + activePeerConnections.size()
        );
    }

    private boolean isConnectionUsable(ClientConnection connection) {
        if (connection == null) {
            return false;
        }

        try {
            return connection.getSocket() != null
                    && connection.getSocket().isConnected()
                    && !connection.getSocket().isClosed()
                    && !connection.getSocket().isInputShutdown()
                    && !connection.getSocket().isOutputShutdown();
        } catch (RuntimeException exception) {
            return false;
        }
    }

    @Override
    public String serialize(Message message) {

        String serializedMessage = messageParser.createJSONFromMessage(
                message
        );

        log.debug(() ->
                "[TCPApplicationProcessor][serialize] Message serialized"
                        + " | operation=" + message.getOperation()
                        + " | serializedLength="
                        + (serializedMessage == null ? 0 : serializedMessage.length())
        );

        return serializedMessage;
    }
}
