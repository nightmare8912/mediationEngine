
package org.prats.services.tcpService;

import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.ProcessContext;
import org.prats.definitions.enums.HealthStatus;
import org.prats.definitions.interfaces.HealthCheckable;
import org.prats.definitions.interfaces.Service;
import org.prats.definitions.interfaces.TCPProcessor;
import org.prats.definitions.records.HealthReport;
import org.prats.definitions.records.TCPPeer;
import org.prats.loggers.EngineLogger;
import org.prats.threading.ThreadPool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

// listens to incoming tcp requests and forwards it to the message dispatcher
// internal services can also use tcp service to communicate to external jvm's
// not my favorite service (as i dont like the way this service got implemented), but this is probably the most important service in the entire jvm
// any jvm (apart from watchdogs) will have at-least two instances of tcp service running
// one of them will be for normal application level processing, the other one is for the communications
// that happens on the control channel.
// the control channel will be the using which the platform will communicate with the container jvm.
// there are three modes of the service.
// 1. SERVER_AND_PEERS: this is for normal application level scenario, all the ec-ec communication or shell
// communication happens through it. if the request is coming on normal tcp port,
// the request gets forwarded to the internal message dispatcher which is nothing
// but the message service. if an outgoing request/reply comes from internal services, it will be routed by
// the tcp service on the appropriate socket (which should be provided with the response)
// 2. SERVER_ONLY: this mode is for the command channel and only for the platform.
// platform will provide a server socket and other container jvms must connect to it.
// the platform expects a heartbeat from the containers through this socket.
// 3. CLIENT_ONLY: however, if the service is operating on client_only mode, that means it is
// still the command port but listener. so no server sockets. this is the mode that all the containers
// should have except the platform container. and this is where every container is expected to send heartbeats
// to the platform. this is also the port using which the watchdog which is the process that creates the container
// on remote machines, connect to the platform over the network.

public class TCPService implements Service, HealthCheckable {

    private static final EngineLogger log =
            EngineLogger.getLogger(TCPService.class);

    // =========================================================
    // Configuration
    // =========================================================

    private final int coreThreads;
    private final int maxThreads;
    private final long idleTimeout;

    private final String bindHost;
    private final int tcpPort;
    private final int tcpBacklog;
    private final boolean hasServerSocket;

    private final TCPProcessor tcpProcessor;
    private final int maxFrameChars;

    private final int outboundBudget =
            Integer.getInteger(
                    "mediation.tcp.max-outbound-bytes",
                    16 * 1024 * 1024
            );

    private final java.util.concurrent.Semaphore outboundBytes =
            new java.util.concurrent.Semaphore(outboundBudget);

    private final Object admissionLock = new Object();
    private final Object peerSchedulerLock = new Object();

    private final String processName =
            ProcessContext.getProcessName();

    private final Set<Socket> activeConnections =
            ConcurrentHashMap.newKeySet();

    // =========================================================
    // Connection Metrics
    // =========================================================

    private final AtomicLong acceptedConnections = new AtomicLong();
    private final AtomicLong connectionInitializationFailures = new AtomicLong();
    private final AtomicLong connectionTaskRejections = new AtomicLong();
    private final AtomicLong clientReadFailures = new AtomicLong();
    private final AtomicLong acceptFailures = new AtomicLong();

    // =========================================================
    // Message Metrics
    // =========================================================

    private final AtomicLong messagesSubmitted = new AtomicLong();
    private final AtomicLong messagesSent = new AtomicLong();
    private final AtomicLong messagesReceived = new AtomicLong();
    private final AtomicLong messagesFailures = new AtomicLong();
    private final AtomicLong messagesRejection = new AtomicLong();

    // =========================================================
    // Executor Metrics
    // =========================================================

    private final AtomicLong submittedTasks = new AtomicLong();
    private final AtomicLong completedTasks = new AtomicLong();
    private final AtomicLong failedTasks = new AtomicLong();
    private final AtomicLong rejectedTasks = new AtomicLong();

    private final AtomicInteger activeTasks = new AtomicInteger();
    private final AtomicInteger outstandingTasks = new AtomicInteger();

    private long lastReportedFailedTasks;
    private long lastReportedRejectedTasks;
    private long lastReportedAcceptFailures;

    // =========================================================
    // Runtime Resources
    // =========================================================

    private ExecutorService tcpWorkers;
    private ScheduledExecutorService backgroundJobScheduler;

    private Thread listenerThread;
    private ServerSocket serverSocket;

    private volatile boolean isQuiescing;

    // =========================================================
    // Constructors
    // =========================================================

    public TCPService(
            int coreThreads,
            int maxThreads,
            long idleTimeout,
            int tcpPort,
            int tcpBacklog,
            boolean hasServerSocket,
            TCPProcessor tcpProcessor
    ) {

        this(
                coreThreads,
                maxThreads,
                idleTimeout,
                InetAddress.getLoopbackAddress().getHostAddress(),
                tcpPort,
                tcpBacklog,
                hasServerSocket,
                tcpProcessor
        );
    }

    public TCPService(
            int coreThreads,
            int maxThreads,
            long idleTimeout,
            String bindHost,
            int tcpPort,
            int tcpBacklog,
            boolean hasServerSocket,
            TCPProcessor tcpProcessor
    ) {

        this(
                coreThreads,
                maxThreads,
                idleTimeout,
                bindHost,
                tcpPort,
                tcpBacklog,
                hasServerSocket,
                tcpProcessor,
                Integer.getInteger(
                        "mediation.tcp.max-frame-chars",
                        65536
                )
        );
    }

    public TCPService(
            int coreThreads,
            int maxThreads,
            long idleTimeout,
            String bindHost,
            int tcpPort,
            int tcpBacklog,
            boolean hasServerSocket,
            TCPProcessor tcpProcessor,
            int maxFrameChars
    ) {

        if (maxFrameChars < 1) {
            throw new IllegalArgumentException(
                    "Frame limit must be positive"
            );
        }

        this.maxFrameChars = maxFrameChars;

        log.debug(() ->
                "[TCPService][constructor] Initializing TCPService"
                        + " | processName=" + processName
                        + " | coreThreads=" + coreThreads
                        + " | maxThreads=" + maxThreads
                        + " | idleTimeout=" + idleTimeout
                        + " | bindHost=" + bindHost
                        + " | tcpPort=" + tcpPort
                        + " | tcpBacklog=" + tcpBacklog
                        + " | hasServerSocket=" + hasServerSocket
                        + " | maxFrameChars=" + maxFrameChars
                        + " | outboundBudget=" + outboundBudget
        );

        this.isQuiescing = true;

        this.coreThreads = coreThreads;
        this.maxThreads = maxThreads;
        this.idleTimeout = idleTimeout;

        this.bindHost = Objects.requireNonNull(
                bindHost,
                "bindHost"
        );

        this.tcpPort = tcpPort;
        this.tcpBacklog = tcpBacklog;
        this.hasServerSocket = hasServerSocket;
        this.tcpProcessor = tcpProcessor;

        log.info(() ->
                "[TCPService][constructor] TCPService initialized"
                        + " | processName=" + processName
                        + " | tcpPort=" + tcpPort
                        + " | hasServerSocket=" + hasServerSocket
                        + " | processorType="
                        + (tcpProcessor == null
                        ? "null"
                        : tcpProcessor.getClass().getSimpleName())
        );
    }

    // =========================================================
    // Connection Diagnostics
    // =========================================================

    public List<Map<String, Object>> connectionDetails() {

        log.debug(() ->
                "[TCPService][connectionDetails] Collecting connection details"
                        + " | activeConnections=" + activeConnections.size()
        );

        Set<Socket> sockets = new HashSet<>(activeConnections);

        sockets.addAll(tcpProcessor.connectedSockets());

        List<Map<String, Object>> details =
                sockets.stream()
                        .filter(socket -> !socket.isClosed())
                        .map(socket -> {

                            Map<String, Object> row =
                                    new LinkedHashMap<>();

                            row.put(
                                    "local",
                                    String.valueOf(socket.getLocalSocketAddress())
                            );

                            row.put(
                                    "remote",
                                    String.valueOf(socket.getRemoteSocketAddress())
                            );

                            row.put(
                                    "state",
                                    socket.isConnected()
                                            ? "CONNECTED"
                                            : "CONNECTING"
                            );

                            row.put("service", getName());

                            return row;
                        })
                        .toList();

        log.debug(() ->
                "[TCPService][connectionDetails] Connection details collected"
                        + " | totalConnections=" + details.size()
        );

        return details;
    }

    /*
     * TCPService standard functions
     * */

    // the tcp listener thread will be constantly listen on this port, and accepting connections. Once a connection is accepted
    // the listener thread will submit the socket to tcpWorker pool for it to handle and it will return back to listening.

    public void listenOnServerSocket() {

        log.info(() ->
                "[TCPService][listenOnServerSocket] TCP listener started"
                        + " | processName=" + processName
                        + " | tcpPort=" + tcpPort
        );

        while (!isQuiescing) {

            Socket clientSocket = null;

            try {

                log.debug(() ->
                        "[TCPService][listenOnServerSocket] Waiting for incoming TCP connection"
                                + " | tcpPort=" + tcpPort
                );

                clientSocket = serverSocket.accept();

                long connectionCount =
                        acceptedConnections.incrementAndGet();

                clientSocket.setKeepAlive(true);

                if (tcpProcessor instanceof
                        org.prats.processors.TCPApplicationProcessor) {

                    clientSocket.setSoTimeout(
                            Integer.getInteger(
                                    "mediation.tcp.read-idle-seconds",
                                    300
                            ) * 1000
                    );
                }

                Socket acceptedSocket = clientSocket;

                log.debug(() ->
                        "[TCPService][listenOnServerSocket] TCP connection accepted"
                                + " | remoteAddress="
                                + acceptedSocket.getRemoteSocketAddress()
                                + " | acceptedConnections=" + connectionCount
                );

                synchronized (admissionLock) {

                    if (isQuiescing) {

                        log.debug(
                                "[TCPService][listenOnServerSocket] "
                                        + "Closing accepted connection because service is quiescing"
                        );

                        closeQuietly(clientSocket);
                        break;
                    }

                    int maxConnections =
                            Integer.getInteger(
                                    "mediation.tcp.max-connections",
                                    256
                            );

                    if (activeConnections.size() >= maxConnections) {

                        long rejectionCount =
                                connectionTaskRejections.incrementAndGet();

                        log.warn(() ->
                                "[TCPService][listenOnServerSocket] Connection limit reached"
                                        + " | maxConnections=" + maxConnections
                                        + " | activeConnections=" + activeConnections.size()
                                        + " | rejectionCount=" + rejectionCount
                                        + " | remoteAddress="
                                        + acceptedSocket.getRemoteSocketAddress()
                        );

                        closeQuietly(clientSocket);
                        continue;
                    }

                    activeConnections.add(clientSocket);

                    try {

                        log.debug(() ->
                                "[TCPService][listenOnServerSocket] Submitting accepted connection"
                                        + " | remoteAddress="
                                        + acceptedSocket.getRemoteSocketAddress()
                                        + " | activeConnections=" + activeConnections.size()
                        );

                        submitToThreadPool(
                                () -> acceptClient(acceptedSocket)
                        );

                    } catch (RejectedExecutionException exception) {

                        long rejectionCount =
                                connectionTaskRejections.incrementAndGet();

                        activeConnections.remove(acceptedSocket);
                        closeQuietly(acceptedSocket);

                        log.warn(() ->
                                "[TCPService][listenOnServerSocket] Connection rejected by TCP worker executor"
                                        + " | remoteAddress="
                                        + acceptedSocket.getRemoteSocketAddress()
                                        + " | connectionTaskRejections=" + rejectionCount
                                        + " | exception=" + exception
                        );
                    }
                }

            } catch (IOException exception) {

                if (!isQuiescing) {

                    long failureCount =
                            acceptFailures.incrementAndGet();

                    log.error(
                            "[TCPService][listenOnServerSocket] TCP connection acceptance failed"
                                    + " | acceptFailures=" + failureCount
                                    + " | tcpPort=" + tcpPort,
                            exception
                    );
                }

                break;
            }
        }

        log.info(() ->
                "[TCPService][listenOnServerSocket] TCP listener stopped"
                        + " | processName=" + processName
                        + " | tcpPort=" + tcpPort
                        + " | isQuiescing=" + isQuiescing
        );
    }

    // =========================================================
    // Bounded Message Reading
    // =========================================================

    private String readBoundedLine(
            BufferedReader reader
    ) throws IOException {

        int limit = maxFrameChars;

        StringBuilder line = new StringBuilder();

        int value;

        while ((value = reader.read()) != -1) {

            if (value == '\n') {
                return line.toString();
            }

            if (value == '\r') {
                continue;
            }

            if (line.length() >= limit) {

                log.warn(() ->
                        "[TCPService][readBoundedLine] Incoming TCP frame exceeded configured limit"
                                + " | maxFrameChars=" + limit
                );

                throw new IOException(
                        "TCP frame exceeds configured size limit"
                );
            }

            line.append((char) value);
        }

        return line.isEmpty()
                ? null
                : line.toString();
    }

    // =========================================================
    // Accepted Client Initialization
    // =========================================================

    private void acceptClient(Socket clientSocket) {

        log.debug(() ->
                "[TCPService][acceptClient] Initializing client connection"
                        + " | remoteAddress="
                        + clientSocket.getRemoteSocketAddress()
        );

        try {

            ClientConnection connection =
                    new ClientConnection(clientSocket);

            log.debug(() ->
                    "[TCPService][acceptClient] Client connection initialized"
                            + " | remoteAddress="
                            + clientSocket.getRemoteSocketAddress()
            );

            handleClient(connection);

        } catch (IOException exception) {

            long failureCount =
                    connectionInitializationFailures.incrementAndGet();

            log.error(
                    "[TCPService][acceptClient] Client initialization failed"
                            + " | connectionInitializationFailures=" + failureCount,
                    exception
            );

            closeQuietly(clientSocket);

        } finally {

            activeConnections.remove(clientSocket);

            log.debug(() ->
                    "[TCPService][acceptClient] Client connection processing finished"
                            + " | remoteAddress="
                            + clientSocket.getRemoteSocketAddress()
                            + " | activeConnections=" + activeConnections.size()
            );
        }
    }

    // reads incoming messages from one client, a tcp virtual thread gets assigned to it which is responsible to handle this
    // client and all the messages that are associated with it

    private void handleClient(
            ClientConnection clientConnection
    ) {

        log.debug(
                "[TCPService][handleClient] Client message handling started"
        );

        try (
                Socket socket = clientConnection.getSocket();

                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(
                                socket.getInputStream(),
                                StandardCharsets.UTF_8
                        )
                )
        ) {

            log.debug(() ->
                    "[TCPService][handleClient] Client socket opened for reading"
                            + " | remoteAddress="
                            + socket.getRemoteSocketAddress()
            );

            String rawMessage;

            while (!isQuiescing &&
                    (rawMessage = readBoundedLine(reader)) != null) {

                long receivedCount =
                        messagesReceived.incrementAndGet();

                final int messageLength = rawMessage.length();

                log.debug(() ->
                        "[TCPService][handleClient] Message received from client"
                                + " | messageLength=" + messageLength
                                + " | messagesReceived=" + receivedCount
                                + " | remoteAddress="
                                + socket.getRemoteSocketAddress()
                );

                try {

                    tcpProcessor.processIncoming(
                            rawMessage,
                            clientConnection
                    );

                    long submittedCount =
                            messagesSubmitted.incrementAndGet();

                    log.debug(() ->
                            "[TCPService][handleClient] Incoming message submitted to TCP processor"
                                    + " | messageLength=" + messageLength
                                    + " | messagesSubmitted=" + submittedCount
                    );

                } catch (RejectedExecutionException exception) {

                    long rejectionCount =
                            messagesRejection.incrementAndGet();

                    log.warn(() ->
                            "[TCPService][handleClient] Message submission rejected"
                                    + " | messagesRejection=" + rejectionCount
                                    + " | exception=" + exception
                    );

                } catch (Exception exception) {

                    log.error(
                            "[TCPService][handleClient] Failed to process incoming message"
                                    + " | messageLength=" + messageLength,
                            exception
                    );
                }
            }

            log.debug(() ->
                    "[TCPService][handleClient] Client read loop completed"
                            + " | remoteAddress="
                            + socket.getRemoteSocketAddress()
                            + " | isQuiescing=" + isQuiescing
            );

        } catch (IOException exception) {

            long failureCount =
                    clientReadFailures.incrementAndGet();

            if (!isQuiescing) {

                log.warn(() ->
                        "[TCPService][handleClient] Client read failed"
                                + " | clientReadFailures=" + failureCount
                                + " | exception=" + exception
                );
            }

        } finally {

            // Remove the socket from transport-level tracking.
            activeConnections.remove(
                    clientConnection.getSocket()
            );

            log.debug(() ->
                    "[TCPService][handleClient] Client removed from active connection tracking"
                            + " | activeConnections=" + activeConnections.size()
            );

            // Notify the processor that this connection is gone.
            try {

                if (tcpProcessor != null) {

                    log.debug(
                            "[TCPService][handleClient] Notifying TCP processor about closed connection"
                    );

                    tcpProcessor.onConnectionClosed(clientConnection);

                    log.debug(
                            "[TCPService][handleClient] TCP processor notified about closed connection"
                    );
                }

            } catch (Exception exception) {

                log.error(
                        "[TCPService][handleClient] Connection-close notification failed",
                        exception
                );
            }

            log.debug(
                    "[TCPService][handleClient] Client handling completed"
            );
        }
    }

    // writes the message on the given connection. the internal call is synchronized so we only have one
    // thread writing on a socket at a time.

    public CompletableFuture<Void> writeMessage(
            String message,
            ClientConnection clientConnection
    ) {

        log.debug(() ->
                "[TCPService][writeMessage] Outgoing message write requested"
                        + " | messageLength="
                        + (message == null ? -1 : message.length())
                        + " | clientConnectionPresent="
                        + (clientConnection != null)
                        + " | outboundBudget=" + outboundBudget
                        + " | availableOutboundBytes="
                        + outboundBytes.availablePermits()
        );

        CompletableFuture<Void> completion =
                new CompletableFuture<>();

        /*
         * Estimate the memory retained by the outgoing message.
         *
         * The semaphore limits the aggregate estimated memory
         * occupied by pending outbound writes.
         */
        long estimatedBytes =
                512L + (message == null ? 0 : message.length() * 2L);

        log.debug(() ->
                "[TCPService][writeMessage] Evaluating outbound byte budget"
                        + " | estimatedBytes=" + estimatedBytes
                        + " | outboundBudget=" + outboundBudget
                        + " | availablePermits="
                        + outboundBytes.availablePermits()
        );

        /*
         * Reject writes that exceed the configured outbound
         * memory budget or cannot acquire sufficient permits.
         */
        if (estimatedBytes > outboundBudget ||
                !outboundBytes.tryAcquire((int) estimatedBytes)) {

            long rejectionCount =
                    messagesRejection.incrementAndGet();

            log.warn(() ->
                    "[TCPService][writeMessage] Outbound write rejected"
                            + " | reason=OUTBOUND_BYTE_BUDGET_EXCEEDED"
                            + " | estimatedBytes=" + estimatedBytes
                            + " | outboundBudget=" + outboundBudget
                            + " | availablePermits="
                            + outboundBytes.availablePermits()
                            + " | messagesRejection=" + rejectionCount
            );

            return CompletableFuture.failedFuture(
                    new RejectedExecutionException(
                            "Outbound byte budget reached"
                    )
            );
        }

        log.debug(() ->
                "[TCPService][writeMessage] Outbound byte permits acquired"
                        + " | acquiredPermits=" + estimatedBytes
                        + " | remainingPermits="
                        + outboundBytes.availablePermits()
        );

        /*
         * Configure an upper bound on the completion future.
         *
         * When the future completes, release the reserved
         * outbound byte permits.
         *
         * A timed-out write also triggers connection closure.
         */
        int writeTimeoutSeconds =
                Integer.getInteger(
                        "mediation.tcp.write-timeout-seconds",
                        10
                );

        log.debug(() ->
                "[TCPService][writeMessage] Configuring write timeout"
                        + " | timeoutSeconds=" + writeTimeoutSeconds
                        + " | estimatedBytes=" + estimatedBytes
        );

        completion
                .orTimeout(
                        writeTimeoutSeconds,
                        TimeUnit.SECONDS
                )
                .whenComplete((ignored, error) -> {

                    outboundBytes.release((int) estimatedBytes);

                    log.debug(() ->
                            "[TCPService][writeMessage] Outbound byte permits released"
                                    + " | releasedPermits=" + estimatedBytes
                                    + " | availablePermits="
                                    + outboundBytes.availablePermits()
                                    + " | completedExceptionally="
                                    + (error != null)
                    );

                    if (error instanceof TimeoutException) {

                        log.warn(() ->
                                "[TCPService][writeMessage] Outbound write timed out"
                                        + " | timeoutSeconds=" + writeTimeoutSeconds
                                        + " | clientConnectionPresent="
                                        + (clientConnection != null)
                        );

                        if (clientConnection != null) {

                            log.debug(
                                    "[TCPService][writeMessage] Closing slow client connection"
                            );

                            closeQuietly(clientConnection.getSocket());

                            log.debug(
                                    "[TCPService][writeMessage] Slow client connection closure requested"
                            );
                        }
                    }

                    if (error != null) {

                        log.debug(() ->
                                "[TCPService][writeMessage] Completion future finished exceptionally"
                                        + " | exception="
                                        + error.getClass().getSimpleName()
                                        + " | reason=" + error.getMessage()
                        );

                    } else {

                        log.debug(
                                "[TCPService][writeMessage] Completion future finished successfully"
                        );
                    }
                });

        /*
         * Submit the actual socket write operation to the
         * TCP service worker executor.
         */
        try {

            log.debug(() ->
                    "[TCPService][writeMessage] Submitting outbound write to worker executor"
                            + " | estimatedBytes=" + estimatedBytes
            );

            submitToThreadPool(() -> {

                log.debug(() ->
                        "[TCPService][writeMessage] Outbound write task started"
                                + " | thread=" + Thread.currentThread()
                                + " | messageLength="
                                + (message == null ? -1 : message.length())
                );

                try {

                    /*
                     * Write the message directly to the client socket.
                     */
                    log.debug(() ->
                            "[TCPService][writeMessage] Writing message to client socket"
                                    + " | messageLength="
                                    + (message == null ? -1 : message.length())
                    );

                    clientConnection.sendMessage(message);

                    long sentCount =
                            messagesSent.incrementAndGet();

                    log.debug(() ->
                            "[TCPService][writeMessage] Socket write completed successfully"
                                    + " | messagesSent=" + sentCount
                                    + " | messageLength="
                                    + (message == null ? -1 : message.length())
                    );

                    /*
                     * Complete the future successfully.
                     *
                     * complete() returns false if the future
                     * was already completed, for example,
                     * because its timeout elapsed.
                     */
                    boolean completedNow =
                            completion.complete(null);

                    log.debug(() ->
                            "[TCPService][writeMessage] Write completion attempted"
                                    + " | completedNow=" + completedNow
                                    + " | futureDone=" + completion.isDone()
                                    + " | completedExceptionally="
                                    + completion.isCompletedExceptionally()
                    );

                } catch (Exception exception) {

                    long failureCount =
                            messagesFailures.incrementAndGet();

                    boolean completedNow =
                            completion.completeExceptionally(exception);

                    log.error(
                            "[TCPService][writeMessage] Outbound socket write failed"
                                    + " | messagesFailures=" + failureCount
                                    + " | messageLength="
                                    + (message == null ? -1 : message.length())
                                    + " | futureCompletedNow=" + completedNow
                                    + " | exception="
                                    + exception.getClass().getSimpleName(),
                            exception
                    );

                } finally {

                    log.debug(() ->
                            "[TCPService][writeMessage] Outbound write task finalized"
                                    + " | futureDone=" + completion.isDone()
                                    + " | completedExceptionally="
                                    + completion.isCompletedExceptionally()
                    );
                }
            });

            long submittedCount =
                    messagesSubmitted.incrementAndGet();

            log.debug(() ->
                    "[TCPService][writeMessage] Outbound write task submitted successfully"
                            + " | messagesSubmitted=" + submittedCount
                            + " | estimatedBytes=" + estimatedBytes
                            + " | availableOutboundBytes="
                            + outboundBytes.availablePermits()
            );

        } catch (RejectedExecutionException exception) {

            /*
             * The worker executor rejected the write task.
             *
             * Completing the future exceptionally triggers
             * the registered completion handler, which releases
             * the outbound byte permits.
             */
            long rejectionCount =
                    messagesRejection.incrementAndGet();

            boolean completedNow =
                    completion.completeExceptionally(exception);

            log.warn(() ->
                    "[TCPService][writeMessage] Worker executor rejected outbound write"
                            + " | messagesRejection=" + rejectionCount
                            + " | estimatedBytes=" + estimatedBytes
                            + " | futureCompletedNow=" + completedNow
                            + " | exception="
                            + exception.getClass().getSimpleName()
                            + " | reason=" + exception.getMessage()
            );
        }

        log.debug(() ->
                "[TCPService][writeMessage] Returning asynchronous completion future"
                        + " | futureDone=" + completion.isDone()
                        + " | completedExceptionally="
                        + completion.isCompletedExceptionally()
        );

        return completion;
    }

    /*
     * Checks the health of the service, gets executed on the health checker thread
     * */

    @Override
    public HealthReport checkHealth() {

        log.debug(() ->
                "[TCPService][checkHealth] TCP service health check started"
                        + " | processName=" + processName
                        + " | tcpPort=" + tcpPort
                        + " | isQuiescing=" + isQuiescing
        );

        Map<String, Object> metrics = new LinkedHashMap<>();

        metrics.put(
                "outboundRetainedBytes",
                outboundBudget - outboundBytes.availablePermits()
        );

        metrics.put("outboundByteLimit", outboundBudget);

        metrics.putAll(
                ThreadPool.admissionMetrics(tcpWorkers)
        );

        metrics.put("isQuiescing", isQuiescing);
        metrics.put("serverSocketRequired", hasServerSocket);

        metrics.put("tcpPort", tcpPort);
        metrics.put("tcpBacklog", tcpBacklog);

        metrics.put(
                "serverSocketInitialized",
                serverSocket != null
        );

        metrics.put(
                "serverSocketBound",
                serverSocket != null && serverSocket.isBound()
        );

        metrics.put(
                "serverSocketClosed",
                serverSocket == null || serverSocket.isClosed()
        );

        metrics.put(
                "listenerInitialized",
                listenerThread != null
        );

        metrics.put(
                "listenerAlive",
                listenerThread != null && listenerThread.isAlive()
        );

        metrics.put(
                "executorInitialized",
                tcpWorkers != null
        );

        metrics.put(
                "executorShutdown",
                tcpWorkers != null && tcpWorkers.isShutdown()
        );

        metrics.put(
                "executorTerminated",
                tcpWorkers != null && tcpWorkers.isTerminated()
        );

        metrics.put(
                "acceptedConnections",
                acceptedConnections.get()
        );

        metrics.put(
                "activeConnections",
                activeConnections.size()
        );

        metrics.put(
                "connectionInitializationFailures",
                connectionInitializationFailures.get()
        );

        metrics.put(
                "connectionTaskRejections",
                connectionTaskRejections.get()
        );

        metrics.put(
                "clientReadFailures",
                clientReadFailures.get()
        );

        metrics.put(
                "acceptFailures",
                acceptFailures.get()
        );

        metrics.put(
                "messagesSubmitted",
                messagesSubmitted.get()
        );

        metrics.put(
                "messagesSent",
                messagesSent.get()
        );

        metrics.put(
                "messagesFailures",
                messagesFailures.get()
        );

        metrics.put(
                "messagesRejection",
                messagesRejection.get()
        );

        metrics.put(
                "submittedTasks",
                submittedTasks.get()
        );

        metrics.put(
                "completedTasks",
                completedTasks.get()
        );

        metrics.put(
                "failedTasks",
                failedTasks.get()
        );

        metrics.put(
                "rejectedTasks",
                rejectedTasks.get()
        );

        metrics.put(
                "activeTasks",
                activeTasks.get()
        );

        metrics.put(
                "outstandingTasks",
                outstandingTasks.get()
        );

        HealthStatus status;
        String message;

        if (tcpWorkers == null ||
                (hasServerSocket && serverSocket == null)) {

            status = HealthStatus.UNREACHABLE;
            message = "TCP service has not been initialized.";

            log.warn(
                    "[TCPService][checkHealth] TCP service has not been initialized"
            );

        } else if (tcpWorkers.isTerminated() ||
                tcpWorkers.isShutdown()) {

            status = HealthStatus.UNHEALTHY;

            message =
                    "TCP worker executor is no longer operational.";

            log.error(
                    "[TCPService][checkHealth] TCP worker executor is no longer operational"
            );

        } else if (!isQuiescing &&
                hasServerSocket &&
                (serverSocket.isClosed() ||
                        listenerThread == null ||
                        !listenerThread.isAlive())) {

            status = HealthStatus.UNHEALTHY;

            message =
                    "TCP listener is no longer accepting connections.";

            log.error(
                    "[TCPService][checkHealth] TCP listener is no longer accepting connections"
            );

        } else {

            boolean newTaskFailures =
                    failedTasks.get() > lastReportedFailedTasks;

            boolean newTaskRejections =
                    rejectedTasks.get() > lastReportedRejectedTasks;

            boolean newAcceptFailures =
                    acceptFailures.get() > lastReportedAcceptFailures;

            log.debug(() ->
                    "[TCPService][checkHealth] Evaluating task and connection failure counters"
                            + " | newTaskFailures=" + newTaskFailures
                            + " | newTaskRejections=" + newTaskRejections
                            + " | newAcceptFailures=" + newAcceptFailures
                            + " | failedTasks=" + failedTasks.get()
                            + " | rejectedTasks=" + rejectedTasks.get()
                            + " | acceptFailures=" + acceptFailures.get()
            );

            lastReportedFailedTasks = failedTasks.get();
            lastReportedRejectedTasks = rejectedTasks.get();
            lastReportedAcceptFailures = acceptFailures.get();

            if (newTaskFailures ||
                    newTaskRejections ||
                    newAcceptFailures) {

                status = HealthStatus.DEGRADED;

                message =
                        "TCP service is operational but has experienced task failures, connection acceptance failures or message dispatch rejections.";

                log.warn(
                        "[TCPService][checkHealth] TCP service is operational but degraded"
                );

            } else {

                status = HealthStatus.HEALTHY;

                message = "TCP service is operating normally.";

                log.debug(
                        "[TCPService][checkHealth] TCP service is healthy"
                );
            }
        }

        final HealthStatus finalStatus = status;

        log.debug(() ->
                "[TCPService][checkHealth] TCP service health evaluation completed"
                        + " | status=" + finalStatus
                        + " | acceptedConnections=" + acceptedConnections.get()
                        + " | activeConnections=" + activeConnections.size()
                        + " | messagesReceived=" + messagesReceived.get()
                        + " | messagesSubmitted=" + messagesSubmitted.get()
                        + " | messagesSent=" + messagesSent.get()
                        + " | messagesFailures=" + messagesFailures.get()
                        + " | messagesRejection=" + messagesRejection.get()
                        + " | activeTasks=" + activeTasks.get()
                        + " | outstandingTasks=" + outstandingTasks.get()
        );

        return new HealthReport(
                "TCPService",
                status,
                message,
                Instant.now(),
                metrics
        );
    }

    // tries to connect to the given tcp peer, on its specified port and host

    public ClientConnection attemptConnection(
            TCPPeer peer
    ) {

        String peerId = peer.processId();

        log.debug(() ->
                "[TCPService][attemptConnection] Peer connection requested"
                        + " | peerId=" + peerId
                        + " | host=" + peer.host()
                        + " | port=" + peer.port()
        );

        if (isQuiescing) {

            log.debug(() ->
                    "[TCPService][attemptConnection] Connection skipped because TCPService is quiescing"
                            + " | peerId=" + peerId
            );

            return null;
        }

        Socket socket = new Socket();
        ClientConnection connection = null;

        try {

            log.debug(() ->
                    "[TCPService][attemptConnection] Connecting to peer"
                            + " | peerId=" + peerId
                            + " | host=" + peer.host()
                            + " | port=" + peer.port()
                            + " | timeoutMillis=5000"
            );

            socket.connect(
                    new InetSocketAddress(
                            peer.host(),
                            peer.port()
                    ),
                    5000
            );

            socket.setKeepAlive(true);

            connection = new ClientConnection(socket);

            long connectionCount =
                    acceptedConnections.incrementAndGet();

            activeConnections.add(socket);

            log.debug(() ->
                    "[TCPService][attemptConnection] Peer socket connected"
                            + " | peerId=" + peerId
                            + " | acceptedConnections=" + connectionCount
                            + " | activeConnections="
                            + activeConnections.size()
            );

            ClientConnection finalConnection = connection;

            submitToThreadPool(
                    () -> handleClient(finalConnection)
            );

            log.info(() ->
                    "[TCPService][attemptConnection] Connected to peer"
                            + " | peerId=" + peerId
                            + " | host=" + peer.host()
                            + " | port=" + peer.port()
            );

        } catch (Exception exception) {

            closeQuietly(socket);

            log.warn(() ->
                    "[TCPService][attemptConnection] Connection failed"
                            + " | peerId=" + peerId
                            + " | host=" + peer.host()
                            + " | port=" + peer.port()
                            + " | exception=" + exception
            );

            return null;
        }

        return connection;
    }

    /*
     * All service lifecycle functions below
     * */

    // =========================================================
    // Startup
    // =========================================================

    @Override
    public synchronized void start() {

        log.info(() ->
                "[TCPService][start] TCPService startup requested"
                        + " | processName=" + processName
                        + " | tcpPort=" + tcpPort
                        + " | tcpBacklog=" + tcpBacklog
                        + " | hasServerSocket=" + hasServerSocket
        );

        if (tcpWorkers != null ||
                backgroundJobScheduler != null ||
                serverSocket != null ||
                listenerThread != null) {

            log.error(
                    "[TCPService][start] TCP service is already started or has not fully shut down"
            );

            throw new IllegalStateException(
                    "TCP service is already started or has not fully shut down."
            );
        }

        ServerSocket newServerSocket = null;
        ExecutorService newTcpWorkers = null;
        ScheduledThreadPoolExecutor newScheduler = null;
        Thread newListener = null;

        try {

            if (hasServerSocket) {

                log.debug(() ->
                        "[TCPService][start] Initializing server socket"
                                + " | tcpPort=" + tcpPort
                                + " | tcpBacklog=" + tcpBacklog
                );

                newServerSocket = new ServerSocket();

                newServerSocket.bind(
                        new InetSocketAddress(
                                InetAddress.getByName(bindHost),
                                tcpPort
                        ),
                        tcpBacklog
                );

                final ServerSocket initializedServerSocket =
                        newServerSocket;

                log.info(() ->
                        "[TCPService][start] Server socket initialized"
                                + " | localAddress="
                                + initializedServerSocket.getLocalSocketAddress()
                );
            }

            log.debug(() ->
                    "[TCPService][start] Initializing TCP worker executor"
                            + " | coreThreads=" + coreThreads
                            + " | maxThreads=" + maxThreads
                            + " | idleTimeout=" + idleTimeout
            );

            newTcpWorkers =
                    new ThreadPool().initializeAndGetThreadPool(
                            coreThreads,
                            maxThreads,
                            idleTimeout,
                            false
                    );

            log.info(
                    "[TCPService][start] TCP worker executor initialized"
            );

            log.debug(
                    "[TCPService][start] Initializing background job scheduler"
            );

            newScheduler = new ScheduledThreadPoolExecutor(1);

            log.info(
                    "[TCPService][start] Background job scheduler initialized"
            );

            tcpWorkers = newTcpWorkers;
            backgroundJobScheduler = newScheduler;
            serverSocket = newServerSocket;

            if (hasServerSocket) {

                ServerSocket listeningSocket =
                        newServerSocket;

                newListener = new Thread(
                        this::listenOnServerSocket,
                        "server-socket-listener"
                );

                listenerThread = newListener;

                final Thread initializedListener =
                        newListener;

                log.debug(() ->
                        "[TCPService][start] Server socket listener thread initialized"
                                + " | threadName="
                                + initializedListener.getName()
                                + " | localAddress="
                                + listeningSocket.getLocalSocketAddress()
                );
            }

            isQuiescing = false;

            log.debug(
                    "[TCPService][start] TCPService admission enabled"
            );

            log.debug(
                    "[TCPService][start] Scheduling cleanup job"
            );

            newScheduler.scheduleWithFixedDelay(
                    this::triggerCleanup,
                    10,
                    60,
                    TimeUnit.SECONDS
            );

            log.info(
                    "[TCPService][start] Cleanup job scheduled successfully"
            );

            log.debug(
                    "[TCPService][start] Scheduling peer reconciliation job"
            );

            newScheduler.scheduleWithFixedDelay(
                    this::triggerReconcilePeer,
                    15,
                    60,
                    TimeUnit.SECONDS
            );

            log.info(
                    "[TCPService][start] Peer reconciliation job scheduled successfully"
            );

            if (newListener != null) {

                log.debug(
                        "[TCPService][start] Starting server socket listener thread"
                );

                newListener.start();

                log.info(
                        "[TCPService][start] Server socket listener thread started"
                );
            }

            log.info(
                    "[TCPService][start] TCP service started successfully."
            );

        } catch (Exception exception) {

            log.error(
                    "[TCPService][start] Startup failed; initiating resource rollback",
                    exception
            );

            isQuiescing = true;

            closeQuietly(newServerSocket);

            if (newListener != null) {

                log.debug(
                        "[TCPService][start] Interrupting partially initialized listener thread"
                );

                newListener.interrupt();
            }

            if (newScheduler != null) {

                log.debug(
                        "[TCPService][start] Shutting down partially initialized scheduler"
                );

                newScheduler.shutdownNow();
            }

            if (newTcpWorkers != null) {

                log.debug(
                        "[TCPService][start] Shutting down partially initialized TCP worker executor"
                );

                newTcpWorkers.shutdownNow();
            }

            boolean interrupted = false;

            try {

                if (newListener != null) {

                    log.debug(
                            "[TCPService][start] Waiting for listener thread termination"
                    );

                    newListener.join(5000);
                }

            } catch (InterruptedException interruptedException) {

                interrupted = true;

                log.warn(() ->
                        "[TCPService][start] Interrupted while waiting for listener termination"
                                + " | exception=" + interruptedException
                );
            }

            try {

                if (newScheduler != null) {

                    log.debug(
                            "[TCPService][start] Waiting for scheduler termination"
                    );

                    newScheduler.awaitTermination(
                            5,
                            TimeUnit.SECONDS
                    );
                }

            } catch (InterruptedException interruptedException) {

                interrupted = true;

                log.warn(() ->
                        "[TCPService][start] Interrupted while waiting for scheduler termination"
                                + " | exception=" + interruptedException
                );
            }

            try {

                if (newTcpWorkers != null) {

                    log.debug(
                            "[TCPService][start] Waiting for TCP worker termination"
                    );

                    newTcpWorkers.awaitTermination(
                            5,
                            TimeUnit.SECONDS
                    );
                }

            } catch (InterruptedException interruptedException) {

                interrupted = true;

                log.warn(() ->
                        "[TCPService][start] Interrupted while waiting for TCP worker termination"
                                + " | exception=" + interruptedException
                );
            }

            if (newServerSocket == null ||
                    newServerSocket.isClosed()) {

                serverSocket = null;
            }

            if (newListener == null ||
                    !newListener.isAlive()) {

                listenerThread = null;
            }

            if (newScheduler == null ||
                    newScheduler.isTerminated()) {

                backgroundJobScheduler = null;
            }

            if (newTcpWorkers == null ||
                    newTcpWorkers.isTerminated()) {

                tcpWorkers = null;
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }

            log.error(
                    "[TCPService][start] Startup failed. Resources rolled back",
                    exception
            );

            throw new IllegalStateException(
                    "Failed to start TCP service.",
                    exception
            );
        }
    }

    // =========================================================
    // Drain
    // =========================================================

    @Override
    public void drain(long timeout) {

        log.info(() ->
                "[TCPService][drain] TCPService drain requested"
                        + " | timeoutMillis=" + timeout
                        + " | activeConnections=" + activeConnections.size()
                        + " | outstandingTasks=" + outstandingTasks.get()
        );

        // stop the listener from accepting new connections
        if (listenerThread != null) {

            log.debug(
                    "[TCPService][drain] Waiting for listener thread to stop"
            );

            try {

                listenerThread.join(timeout);

            } catch (InterruptedException exception) {

                log.warn(() ->
                        "[TCPService][drain] Interrupted while waiting for listener thread"
                                + " | exception=" + exception
                );

                Thread.currentThread().interrupt();
                return;
            }
        }

        // check for work completion for a certain period
        long deadline =
                System.nanoTime()
                        + TimeUnit.MILLISECONDS.toNanos(timeout);

        while (hasOutstandingWork()) {

            if (System.nanoTime() >= deadline) {

                log.warn(() ->
                        "[TCPService][drain] Drain deadline reached with work still outstanding"
                                + " | activeConnections=" + activeConnections.size()
                                + " | outstandingTasks=" + outstandingTasks.get()
                                + " | timeoutMillis=" + timeout
                );

                break;
            }

            LockSupport.parkNanos(
                    TimeUnit.MILLISECONDS.toNanos(100)
            );
        }

        log.debug(() ->
                "[TCPService][drain] TCPService drain operation completed"
                        + " | activeConnections=" + activeConnections.size()
                        + " | outstandingTasks=" + outstandingTasks.get()
        );
    }

    // =========================================================
    // Quiesce
    // =========================================================

    @Override
    public void quiesce() {

        log.info(
                "[TCPService][quiesce] TCPService quiescence requested"
        );

        synchronized (admissionLock) {
            isQuiescing = true;
        }

        log.debug(
                "[TCPService][quiesce] TCPService admission disabled"
        );

        log.info(
                "[TCPService][quiesce] TCPService is now quiescing"
        );
    }

    // =========================================================
    // Shutdown
    // =========================================================

    @Override
    public void shutdown() {

        log.info(
                "[TCPService][shutdown] TCPService shutdown requested"
        );

        isQuiescing = true;

        log.debug(
                "[TCPService][shutdown] TCPService admission disabled"
        );

        if (tcpProcessor != null) {

            log.debug(
                    "[TCPService][shutdown] Informing TCP processor about shutdown"
            );

            tcpProcessor.informShutdown();
        }

        log.debug(
                "[TCPService][shutdown] Closing server socket"
        );

        closeQuietly(serverSocket);

        if (backgroundJobScheduler != null) {

            log.debug(
                    "[TCPService][shutdown] Initiating background scheduler shutdown"
            );

            backgroundJobScheduler.shutdown();
        }

        log.debug(() ->
                "[TCPService][shutdown] Closing active client connections"
                        + " | activeConnections="
                        + activeConnections.size()
        );

        for (Socket socket : activeConnections) {
            closeQuietly(socket);
        }

        if (listenerThread != null) {

            log.debug(
                    "[TCPService][shutdown] Interrupting listener thread"
            );

            listenerThread.interrupt();
        }

        if (tcpWorkers != null) {

            log.debug(
                    "[TCPService][shutdown] Initiating TCP worker executor shutdown"
            );

            tcpWorkers.shutdown();
        }

        boolean interrupted = false;

        try {

            if (listenerThread != null) {

                log.debug(
                        "[TCPService][shutdown] Waiting for listener thread termination"
                );

                listenerThread.join(5000);
            }

            if (backgroundJobScheduler != null &&
                    !backgroundJobScheduler.awaitTermination(
                            5,
                            TimeUnit.SECONDS
                    )) {

                log.warn(
                        "[TCPService][shutdown] Background scheduler did not terminate within graceful shutdown period"
                );

                backgroundJobScheduler.shutdownNow();

                backgroundJobScheduler.awaitTermination(
                        5,
                        TimeUnit.SECONDS
                );
            }

            if (tcpWorkers != null &&
                    !tcpWorkers.awaitTermination(
                            5,
                            TimeUnit.SECONDS
                    )) {

                log.warn(
                        "[TCPService][shutdown] TCP workers did not terminate within graceful shutdown period"
                );

                tcpWorkers.shutdownNow();

                tcpWorkers.awaitTermination(
                        5,
                        TimeUnit.SECONDS
                );
            }

        } catch (InterruptedException exception) {

            interrupted = true;

            log.error(
                    "[TCPService][shutdown] Interrupted while waiting for TCP service resource termination",
                    exception
            );

            if (backgroundJobScheduler != null) {
                backgroundJobScheduler.shutdownNow();
            }

            if (tcpWorkers != null) {
                tcpWorkers.shutdownNow();
            }
        }

        boolean terminated =
                (listenerThread == null ||
                        !listenerThread.isAlive())
                        &&
                        (backgroundJobScheduler == null ||
                                backgroundJobScheduler.isTerminated())
                        &&
                        (tcpWorkers == null ||
                                tcpWorkers.isTerminated());

        if (terminated) {

            activeConnections.clear();

            serverSocket = null;
            listenerThread = null;
            backgroundJobScheduler = null;
            tcpWorkers = null;

            log.info(
                    "[TCPService][shutdown] Shutdown completed. All runtime resources cleared."
            );

        } else {

            log.warn(
                    "[TCPService][shutdown] Shutdown incomplete. Some threads remain active."
            );
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /*
     * Internal utilities for the service
     * */

    // no quiesce check here as this is an internal function, we  assume service must have checked
    // the quiescing state before accepting work from external threads

    private void submitToThreadPool(Runnable task) {

        log.debug(() ->
                "[TCPService][submitToThreadPool] Submitting task to TCP worker executor"
                        + " | activeTasks=" + activeTasks.get()
                        + " | outstandingTasks=" + outstandingTasks.get()
        );

        try {

            long outstandingCount =
                    outstandingTasks.incrementAndGet();

            log.debug(() ->
                    "[TCPService][submitToThreadPool] Outstanding task counter incremented"
                            + " | outstandingTasks=" + outstandingCount
            );

            tcpWorkers.submit(() -> {

                int currentActiveTasks =
                        activeTasks.incrementAndGet();

                log.debug(() ->
                        "[TCPService][submitToThreadPool] Worker task started"
                                + " | activeTasks=" + currentActiveTasks
                                + " | outstandingTasks="
                                + outstandingTasks.get()
                );

                try {

                    task.run();

                    log.debug(
                            "[TCPService][submitToThreadPool] Worker task execution completed"
                    );

                } catch (Exception exception) {

                    long failureCount =
                            failedTasks.incrementAndGet();

                    log.error(
                            "[TCPService][submitToThreadPool] Worker task failed"
                                    + " | failedTasks=" + failureCount,
                            exception
                    );

                    throw exception;

                } finally {

                    long completionCount =
                            completedTasks.incrementAndGet();

                    int remainingActiveTasks =
                            activeTasks.decrementAndGet();

                    int remainingOutstandingTasks =
                            outstandingTasks.decrementAndGet();

                    log.debug(() ->
                            "[TCPService][submitToThreadPool] Worker task counters updated"
                                    + " | completedTasks=" + completionCount
                                    + " | activeTasks=" + remainingActiveTasks
                                    + " | outstandingTasks=" + remainingOutstandingTasks
                    );
                }
            });

            long submittedCount =
                    submittedTasks.incrementAndGet();

            log.debug(() ->
                    "[TCPService][submitToThreadPool] Task submitted successfully"
                            + " | submittedTasks=" + submittedCount
                            + " | outstandingTasks=" + outstandingTasks.get()
            );

        } catch (RejectedExecutionException exception) {

            long outstandingCount =
                    outstandingTasks.decrementAndGet();

            long rejectionCount =
                    rejectedTasks.incrementAndGet();

            log.warn(() ->
                    "[TCPService][submitToThreadPool] Task rejected by TCP worker executor"
                            + " | rejectedTasks=" + rejectionCount
                            + " | outstandingTasks=" + outstandingCount
                            + " | exception=" + exception
            );

            throw exception;
        }
    }

    // used while graceful shutdown by drain
    private boolean hasOutstandingWork() {
        return true;
    }

    private void closeQuietly(AutoCloseable resource) {

        if (resource == null) {

            log.debug(
                    "[TCPService][closeQuietly] Skipping close because resource is null"
            );

            return;
        }

        try {

            log.debug(() ->
                    "[TCPService][closeQuietly] Closing resource"
                            + " | resourceType="
                            + resource.getClass().getSimpleName()
            );

            resource.close();

            log.debug(() ->
                    "[TCPService][closeQuietly] Resource closed successfully"
                            + " | resourceType="
                            + resource.getClass().getSimpleName()
            );

        } catch (Exception exception) {

            log.error(
                    "[TCPService][closeQuietly] Unable to close resource: "
                            + resource.getClass().getSimpleName(),
                    exception
            );
        }
    }

    /*
     * All background jobs below
     * */

    // =========================================================
    // Peer Reconciliation
    // =========================================================

    private void triggerReconcilePeer() {

        log.debug(
                "[TCPService][triggerReconcilePeer] Peer reconciliation job triggered"
        );

        submitToThreadPool(
                this::reconcilePeers
        );

        log.debug(
                "[TCPService][triggerReconcilePeer] Peer reconciliation task submitted"
        );
    }

    private void reconcilePeers() {

        log.debug(
                "[TCPService][reconcilePeers] Starting peer reconciliation"
        );

        tcpProcessor.reconcilePeers();

        log.debug(
                "[TCPService][reconcilePeers] Peer reconciliation completed"
        );
    }

    // =========================================================
    // Correlation Cleanup
    // =========================================================

    private void triggerCleanup() {

        log.debug(
                "[TCPService][triggerCleanup] TCP cleanup job triggered"
        );

        submitToThreadPool(
                this::cleanup
        );

        log.debug(
                "[TCPService][triggerCleanup] TCP cleanup task submitted"
        );
    }

    @Override
    public void cleanup() {

        log.debug(
                "[TCPService][cleanup] Starting correlation cleanup"
        );

        tcpProcessor.cleanupCorrelations();

        log.debug(
                "[TCPService][cleanup] Correlation cleanup completed"
        );
    }
}
