
package org.prats.desktop;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.ProcessType;
import org.prats.definitions.enums.TCPConnectionMode;
import org.prats.definitions.interfaces.ReplyChannel;
import org.prats.definitions.interfaces.TCPMessageHandler;
import org.prats.definitions.records.TCPPeer;
import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;
import org.prats.processors.TCPProtocolProcessor;
import org.prats.services.tcpService.TCPService;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public class DesktopConnectionManager implements AutoCloseable {

    private static final EngineLogger log =
            EngineLogger.getLogger(DesktopConnectionManager.class);

    // =========================================================
    // Constants
    // =========================================================

    private static final TCPConnectionMode TCP_CONNECTION_MODE =
            TCPConnectionMode.CLIENT_ONLY;

    private static final String PLATFORM_PROCESS = "PLATFORM";

    private static final long HANDSHAKE_TIMEOUT_MILLIS = 15_000L;

    private static final long HANDSHAKE_POLL_INTERVAL_MILLIS = 100L;

    // =========================================================
    // Dependencies
    // =========================================================

    private final MessageParser messageParser =
            new MessageParser(ProcessType.GUI);

    /*
     * Optional during initial connection setup.
     *
     * Once the GUI supports incoming notifications and
     * other unsolicited messages, a TCPMessageHandler
     * should be configured.
     */
    private volatile TCPMessageHandler tcpMessageHandler;

    // =========================================================
    // TCP Runtime Resources
    // =========================================================

    /*
     * These resources belong to the desktop application,
     * not to the LoginController.
     *
     * They remain available after login so all desktop
     * controllers can reuse the same Platform connection.
     */
    private volatile TCPProtocolProcessor tcpProcessor;

    private volatile TCPService tcpService;

    /*
     * Stores the connected Platform endpoint.
     *
     * Null indicates that there is no successfully
     * established Platform connection.
     */
    private volatile String platformHost;

    // =========================================================
    // Constructors
    // =========================================================

    public DesktopConnectionManager() {

        log.info(
                "[DesktopConnectionManager][constructor] "
                        + "Desktop connection manager initialized"
        );
    }

    public DesktopConnectionManager(
            TCPMessageHandler tcpMessageHandler
    ) {

        this.tcpMessageHandler = Objects.requireNonNull(
                tcpMessageHandler,
                "TCP message handler cannot be null"
        );

        log.info(() ->
                "[DesktopConnectionManager][constructor] "
                        + "Desktop connection manager initialized"
                        + " | handlerType="
                        + tcpMessageHandler.getClass().getSimpleName()
        );
    }

    // =========================================================
    // TCP Message Handler Configuration
    // =========================================================

    /*
     * Allows the desktop application to supply its
     * incoming message handler independently of the
     * TCP connection lifecycle.
     *
     * If a connection already exists, update the
     * currently active protocol processor as well.
     */
    public synchronized void setTCPMessageHandler(
            TCPMessageHandler tcpMessageHandler
    ) {

        Objects.requireNonNull(
                tcpMessageHandler,
                "TCP message handler cannot be null"
        );

        this.tcpMessageHandler = tcpMessageHandler;

        TCPProtocolProcessor processor = this.tcpProcessor;

        if (processor != null) {

            // Uses the existing setter on TCPProtocolProcessor.
            processor.setControlMessageHandler(tcpMessageHandler);
        }

        log.info(() ->
                "[DesktopConnectionManager][setTCPMessageHandler] "
                        + "TCP message handler configured"
                        + " | handlerType="
                        + tcpMessageHandler.getClass().getSimpleName()
                        + " | processorInitialized="
                        + (processor != null)
        );
    }

    // =========================================================
    // TCP Connection Initialization
    // =========================================================

    /*
     * Creates the TCP protocol processor and TCP service.
     *
     * The GUI operates in CLIENT_ONLY mode because it
     * connects to the Platform rather than listening
     * for incoming TCP connections.
     *
     * The TCP service must be started before the
     * protocol processor attempts peer reconciliation.
     */
    private void initializeTCPConnection(
            TCPPeer tcpPeer
    ) {

        Objects.requireNonNull(
                tcpPeer,
                "TCP peer cannot be null"
        );

        log.debug(() ->
                "[DesktopConnectionManager][initializeTCPConnection] "
                        + "Initializing TCP infrastructure"
                        + " | peerId=" + tcpPeer.processId()
                        + " | host=" + tcpPeer.host()
                        + " | port=" + tcpPeer.port()
                        + " | connectionMode=" + TCP_CONNECTION_MODE
        );

        TCPProtocolProcessor newProcessor =
                new TCPProtocolProcessor(
                        TCP_CONNECTION_MODE,
                        List.of(tcpPeer),
                        messageParser
                );

        /*
         * Configure the desktop message handler when
         * one has been supplied.
         */
        TCPMessageHandler handler = tcpMessageHandler;

        if (handler != null) {

            newProcessor.setControlMessageHandler(handler);

            log.debug(() ->
                    "[DesktopConnectionManager][initializeTCPConnection] "
                            + "Message handler injected"
                            + " | handlerType="
                            + handler.getClass().getSimpleName()
            );

        } else {

            log.warn(
                    "[DesktopConnectionManager][initializeTCPConnection] "
                            + "No TCP message handler configured; "
                            + "unsolicited messages cannot be handled yet"
            );
        }

        /*
         * No server socket is created for the GUI.
         *
         * tcpPeer.port() is passed as the service's TCP
         * port for configuration/diagnostic consistency.
         * It is the Platform's remote port, not a local
         * listening port in CLIENT_ONLY mode.
         */
        TCPService newService =
                new TCPService(
                        2,
                        2,
                        60,
                        tcpPeer.port(),
                        20,
                        false,
                        newProcessor
                );

        /*
         * The protocol processor needs its TCP service
         * dependency before peer reconciliation begins.
         */
        newProcessor.setTcpService(newService);

        try {

            log.debug(
                    "[DesktopConnectionManager][initializeTCPConnection] "
                            + "Starting TCP service"
            );

            newService.start();

            /*
             * Publish the fully initialized resources
             * only after successful startup.
             */
            this.tcpProcessor = newProcessor;
            this.tcpService = newService;

            log.info(() ->
                    "[DesktopConnectionManager][initializeTCPConnection] "
                            + "TCP infrastructure initialized successfully"
                            + " | platformHost=" + tcpPeer.host()
                            + " | platformPort=" + tcpPeer.port()
            );

        } catch (RuntimeException exception) {

            log.error(
                    "[DesktopConnectionManager][initializeTCPConnection] "
                            + "TCP infrastructure initialization failed",
                    exception
            );

            /*
             * Release anything that may have been
             * initialized before startup failed.
             */
            try {

                newService.shutdown();

            } catch (RuntimeException cleanupException) {

                exception.addSuppressed(cleanupException);

                log.error(
                        "[DesktopConnectionManager][initializeTCPConnection] "
                                + "Failed to clean up partially initialized TCP service",
                        cleanupException
                );
            }

            throw exception;
        }
    }

    // =========================================================
    // Platform Endpoint Parsing
    // =========================================================

    /*
     * Accepts an endpoint such as:
     *
     * 127.0.0.1:15000
     * localhost:15000
     * [::1]:15000
     *
     * IPv6 addresses must be enclosed in square brackets.
     */
    private TCPPeer parsePlatformEndpoint(
            String platformHost
    ) {

        if (platformHost == null || platformHost.isBlank()) {

            throw new IllegalArgumentException(
                    "Platform host cannot be empty"
            );
        }

        String endpoint = platformHost.trim();

        int separatorIndex = endpoint.lastIndexOf(':');

        if (separatorIndex <= 0 ||
                separatorIndex == endpoint.length() - 1) {

            throw new IllegalArgumentException(
                    "Invalid Platform address. Expected host:port"
            );
        }

        String host = endpoint.substring(
                0,
                separatorIndex
        ).trim();

        String portValue = endpoint.substring(
                separatorIndex + 1
        ).trim();

        /*
         * Remove IPv6 address brackets while preserving
         * the address itself.
         */
        if (host.startsWith("[") && host.endsWith("]")) {

            host = host.substring(
                    1,
                    host.length() - 1
            );

        } else if (host.contains(":")) {

            throw new IllegalArgumentException(
                    "IPv6 addresses must use [address]:port"
            );
        }

        if (host.isBlank() || host.contains(" ")) {

            throw new IllegalArgumentException(
                    "Invalid Platform hostname"
            );
        }

        int port;

        try {

            port = Integer.parseInt(portValue);

        } catch (NumberFormatException exception) {

            throw new IllegalArgumentException(
                    "Platform port must be a valid integer",
                    exception
            );
        }

        if (port < 1 || port > 65535) {

            throw new IllegalArgumentException(
                    "Platform port must be between 1 and 65535"
            );
        }

        final String parsedHost = host;
        final int parsedPort = port;

        log.debug(() ->
                "[DesktopConnectionManager][parsePlatformEndpoint] "
                        + "Platform endpoint parsed successfully"
                        + " | host=" + parsedHost
                        + " | port=" + parsedPort
        );

        return new TCPPeer(
                PLATFORM_PROCESS,
                parsedHost,
                parsedPort
        );
    }

    // =========================================================
    // Platform Connection
    // =========================================================

    /*
     * Attempts to establish a TCP connection to the
     * Platform and waits for the protocol handshake
     * to complete.
     *
     * IMPORTANT:
     * This method is blocking.
     *
     * Swing controllers should call attemptConnectionAsync()
     * instead of executing this method directly on the EDT.
     *
     * A successful handshake only establishes the TCP
     * connection. User authentication happens separately.
     */
    public synchronized boolean attemptConnection(
            String platformHost
    ) {

        log.info(() ->
                "[DesktopConnectionManager][attemptConnection] "
                        + "Platform connection requested"
                        + " | endpoint=" + platformHost
        );

        TCPPeer tcpPeer;

        try {

            tcpPeer = parsePlatformEndpoint(platformHost);

        } catch (IllegalArgumentException exception) {

            log.warn(() ->
                    "[DesktopConnectionManager][attemptConnection] "
                            + "Invalid Platform endpoint"
                            + " | reason=" + exception.getMessage()
            );

            return false;
        }

        String endpoint =
                tcpPeer.host() + ":" + tcpPeer.port();

        /*
         * Reuse the existing connection if it is still
         * active and points to the same Platform.
         */
        if (endpoint.equals(this.platformHost) && isConnected()) {

            log.info(() ->
                    "[DesktopConnectionManager][attemptConnection] "
                            + "Reusing existing Platform connection"
                            + " | endpoint=" + endpoint
            );

            return true;
        }

        try {

            /*
             * Stop any previous TCP infrastructure before
             * replacing the Platform connection.
             */
            shutdownCurrentConnection();

            initializeTCPConnection(tcpPeer);

            TCPProtocolProcessor processor = tcpProcessor;

            log.debug(() ->
                    "[DesktopConnectionManager][attemptConnection] "
                            + "Starting Platform peer reconciliation"
                            + " | endpoint=" + endpoint
            );

            /*
             * This initiates connection establishment
             * and sends the protocol handshake.
             *
             * It does not guarantee that the handshake
             * has finished when the method returns.
             */
            processor.reconcilePeers();

            boolean connected =
                    awaitPlatformHandshake(
                            processor,
                            HANDSHAKE_TIMEOUT_MILLIS
                    );

            if (!connected) {

                log.warn(() ->
                        "[DesktopConnectionManager][attemptConnection] "
                                + "Platform handshake did not complete"
                                + " | endpoint=" + endpoint
                                + " | timeoutMillis="
                                + HANDSHAKE_TIMEOUT_MILLIS
                );

                shutdownCurrentConnection();

                return false;
            }

            this.platformHost = endpoint;

            log.info(() ->
                    "[DesktopConnectionManager][attemptConnection] "
                            + "Platform TCP connection established"
                            + " | endpoint=" + endpoint
            );

            return true;

        } catch (RuntimeException exception) {

            log.error(
                    "[DesktopConnectionManager][attemptConnection] "
                            + "Unable to establish Platform connection"
                            + " | endpoint=" + endpoint,
                    exception
            );

            try {

                shutdownCurrentConnection();

            } catch (RuntimeException cleanupException) {

                log.error(
                        "[DesktopConnectionManager][attemptConnection] "
                                + "Connection cleanup failed",
                        cleanupException
                );
            }

            return false;
        }
    }

    // =========================================================
    // Asynchronous Platform Connection
    // =========================================================

    /*
     * Starts the connection attempt on a virtual thread.
     *
     * This is intended for Swing controllers so network
     * operations and handshake waiting do not block
     * the Event Dispatch Thread.
     */
    public CompletableFuture<Boolean> attemptConnectionAsync(
            String platformHost
    ) {

        log.debug(() ->
                "[DesktopConnectionManager][attemptConnectionAsync] "
                        + "Scheduling asynchronous Platform connection"
                        + " | endpoint=" + platformHost
        );

        CompletableFuture<Boolean> completion =
                new CompletableFuture<>();

        Thread.ofVirtual()
                .name("desktop-platform-connection")
                .start(() -> {

                    try {

                        boolean connected =
                                attemptConnection(platformHost);

                        completion.complete(connected);

                    } catch (Exception exception) {

                        log.error(
                                "[DesktopConnectionManager][attemptConnectionAsync] "
                                        + "Unexpected connection failure",
                                exception
                        );

                        completion.completeExceptionally(exception);
                    }
                });

        return completion;
    }

    // =========================================================
    // Handshake Completion
    // =========================================================

    /*
     * The protocol processor registers a peer only
     * after successfully resolving its handshake.
     *
     * Wait until that registration is visible or
     * the handshake timeout is reached.
     */
    private boolean awaitPlatformHandshake(
            TCPProtocolProcessor processor,
            long timeoutMillis
    ) {

        log.debug(() ->
                "[DesktopConnectionManager][awaitPlatformHandshake] "
                        + "Waiting for Platform handshake"
                        + " | timeoutMillis=" + timeoutMillis
        );

        long deadline =
                System.nanoTime()
                        + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);

        while (System.nanoTime() < deadline) {

            if (processor.isPeerConnected(PLATFORM_PROCESS)) {

                log.debug(
                        "[DesktopConnectionManager][awaitPlatformHandshake] "
                                + "Platform handshake completed successfully"
                );

                return true;
            }

            try {

                Thread.sleep(HANDSHAKE_POLL_INTERVAL_MILLIS);

            } catch (InterruptedException exception) {

                Thread.currentThread().interrupt();

                log.warn(
                        "[DesktopConnectionManager][awaitPlatformHandshake] "
                                + "Handshake wait interrupted"
                );

                return false;
            }
        }

        /*
         * Final check handles a handshake completing
         * immediately before the deadline.
         */
        boolean connected =
                processor.isPeerConnected(PLATFORM_PROCESS);

        if (!connected) {

            log.warn(() ->
                    "[DesktopConnectionManager][awaitPlatformHandshake] "
                            + "Handshake timed out"
                            + " | timeoutMillis=" + timeoutMillis
            );
        }

        return connected;
    }

    // =========================================================
    // Connection Status
    // =========================================================

    public boolean isConnected() {

        TCPProtocolProcessor processor = tcpProcessor;

        return processor != null &&
                processor.isPeerConnected(PLATFORM_PROCESS);
    }

    public String getPlatformHost() {
        return platformHost;
    }

    // =========================================================
    // Connection Shutdown
    // =========================================================

    /*
     * Stops the existing TCP service and removes its
     * associated processor and endpoint references.
     *
     * Called when switching Platforms, when a connection
     * attempt fails, and during desktop shutdown.
     */
    private void shutdownCurrentConnection() {

        TCPService service = tcpService;

        if (service == null) {

            tcpProcessor = null;
            platformHost = null;

            log.debug(
                    "[DesktopConnectionManager][shutdownCurrentConnection] "
                            + "No active TCP service to shut down"
            );

            return;
        }

        log.info(() ->
                "[DesktopConnectionManager][shutdownCurrentConnection] "
                        + "Stopping TCP infrastructure"
                        + " | endpoint=" + platformHost
        );

        /*
         * TCPService.shutdown() already informs the TCP
         * processor about shutdown and closes its sockets,
         * worker executor, and background scheduler.
         */
        service.shutdown();

        tcpService = null;
        tcpProcessor = null;
        platformHost = null;

        log.info(
                "[DesktopConnectionManager][shutdownCurrentConnection] "
                        + "TCP infrastructure shutdown completed"
        );
    }




    public void sendMessage(
            Message message
    ) {
        // all outgoing messages to platform, gui cannot connect with ECs or watchdogs
        message.setRecipientID(PLATFORM_PROCESS);

        tcpProcessor.processOutgoing(
                message
        );

    }

    public CompletionStage<Void> sendMessage(
            Message message,
            ReplyChannel replyChannel
    ) {

        Objects.requireNonNull(message, "Message cannot be null");
        Objects.requireNonNull(replyChannel, "ReplyChannel cannot be null");

        TCPProtocolProcessor processor = tcpProcessor;

        if (processor == null ||
                !processor.isPeerConnected("PLATFORM")) {

            log.warn(
                    "[DesktopConnectionManager][sendMessage] "
                            + "Cannot send message because Platform is not connected"
            );

            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "Platform is not connected"
                    )
            );
        }

        log.debug(() ->
                "[DesktopConnectionManager][sendMessage] "
                        + "Submitting message to TCP protocol processor"
                        + " | correlationId=" + message.getCorrelationId()
                        + " | operation=" + message.getOperation()
        );

        return processor.sendRequest(message, replyChannel);
    }


    // =========================================================
    // Desktop Application Cleanup
    // =========================================================

    /*
     * The desktop application owns this manager.
     *
     * Individual page controllers must not call close()
     * during normal page navigation, because the same
     * connection is reused across the GUI.
     */
    @Override
    public synchronized void close() {

        log.info(
                "[DesktopConnectionManager][close] "
                        + "Desktop connection manager shutdown requested"
        );

        try {

            shutdownCurrentConnection();

            log.info(
                    "[DesktopConnectionManager][close] "
                            + "Desktop connection manager shutdown completed"
            );

        } catch (RuntimeException exception) {

            log.error(
                    "[DesktopConnectionManager][close] "
                            + "Desktop connection manager shutdown failed",
                    exception
            );

            throw exception;
        }
    }
}
