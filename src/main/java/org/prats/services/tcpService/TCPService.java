//
//package org.prats.services.tcpService;
//
//import com.fasterxml.jackson.databind.node.JsonNodeFactory;
//import com.fasterxml.jackson.databind.node.ObjectNode;
//import org.prats.definitions.classes.ClientConnection;
//import org.prats.definitions.classes.Message;
//import org.prats.definitions.enums.HealthStatus;
//import org.prats.definitions.enums.MessageType;
//import org.prats.definitions.enums.TCPConnectionMode;
//import org.prats.definitions.interfaces.*;
//import org.prats.definitions.records.HealthReport;
//import org.prats.definitions.records.TCPPeer;
//import org.prats.processors.TCPControlProcessor;
//import org.prats.processors.TCPApplicationProcessor;
//import org.prats.replyChannels.TCPReplyChannel;
//import org.prats.loggers.FileLogger;
//import org.prats.threading.ThreadPool;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.net.InetAddress;
//import java.net.InetSocketAddress;
//import java.net.ServerSocket;
//import java.net.Socket;
//import java.nio.charset.StandardCharsets;
//import java.time.Instant;
//import java.util.LinkedHashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicLong;
//import java.util.concurrent.locks.LockSupport;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ScheduledThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//
//// listens to incoming tcp requests and forwards it to the message dispatcher
//// internal services can also use tcp service to communicate to external jvm's
//// not my favorite service (as i dont like the way this service got implemented), but this is probably the most important service in the entire jvm
//// any jvm (apart from watchdogs) will have at-least two instances of tcp service running
//// one of them will be for normal application level processing, the other one is for the communications
//// that happens on the control channel.
//// the control channel will be the using which the platform will communicate with the container jvm.
//// there are three modes of the service.
//// 1. SERVER_AND_PEERS: this is for normal application level scenario, all the ec-ec communication or shell
//// communication happens through it. if the request is coming on normal tcp port,
//// the request gets forwarded to the internal message dispatcher which is nothing
//// but the message service. if an outgoing request/reply comes from internal services, it will be routed by
//// the tcp service on the appropriate socket (which should be provided with the response)
//// 2. SERVER_ONLY: this mode is for the command channel and only for the platform.
//// platform will provide a server socket and other container jvms must connect to it.
//// the platform expects a heartbeat from the containers through this socket.
//// 3. CLIENT_ONLY: however, if the service is operating on client_only mode, that means it is
//// still the command port but listener. so no server sockets. this is the mode that all the containers
//// should have except the platform container. and this is where every container is expected to send heartbeats
//// to the platform. this is also the port using which the watchdog which is the process that creates the container
//// on remote machines, connect to the platform over the network.
//public class TCPService implements Service, Submittable, HealthCheckable, ControlChannelCommunicator {
//
////    public enum ConnectionMode {
////        SERVER_AND_PEERS,
////        SERVER_ONLY,
////        CLIENT_ONLY
////    }
//
//    private static final int CONNECT_TIMEOUT_MILLIS = 5000;
////    private static final int HANDSHAKE_TIMEOUT_MILLIS = 5000;
//
//    private final TCPApplicationProcessor tcpApplicationProcessor;
//    private final TCPConnectionMode connectionMode;
//    private final Object admissionLock;
//
//    private final int coreThreads;
//    private final int maxThreads;
//    private final long idleTimeout;
//    private final int tcpPort;
//    private final int tcpBacklog;
//    private final String processName;
//
//    private final Set<Socket> activeConnections;
//    private final Map<String, ClientConnection> peerConnections;
//    private final List<TCPPeer> peersToConnect;
//
//    private ExecutorService tcpWorkers;
//    private Thread listenerThread;
//    private Thread peerConnectorThread;
//    private ScheduledThreadPoolExecutor backgroundJobScheduler;
//    private ServerSocket serverSocket;
//    private TCPControlProcessor tcpControlProcessor;
//
//    private static final long PEER_RECONCILIATION_INTERVAL_SECONDS = 1;
//
//    private static final long INITIAL_PEER_RETRY_SECONDS = 1;
//
//    private static final long MAX_PEER_RETRY_SECONDS = 30;
//
//    private volatile ScheduledExecutorService peerConnectionScheduler;
//
//    private final Object peerSchedulerLock = new Object();
//
//    private final Set<String> peersConnecting =
//            ConcurrentHashMap.newKeySet();
//
//    private final Map<String, PeerRetryState> peerRetryStates =
//            new ConcurrentHashMap<>();
//
//    private volatile boolean isQuiescing;
//
//    private final AtomicLong acceptedConnections = new AtomicLong();
//    private final AtomicLong connectionInitializationFailures = new AtomicLong();
//    private final AtomicLong connectionTaskRejections = new AtomicLong();
//    private final AtomicLong clientReadFailures = new AtomicLong();
//    private final AtomicLong acceptFailures = new AtomicLong();
//
//    private final AtomicLong messagesSubmitted = new AtomicLong();
//    private final AtomicLong messagesSent = new AtomicLong();
//    private final AtomicLong messagesReceived = new AtomicLong();
//    private final AtomicLong messagesFailures = new AtomicLong();
//    private final AtomicLong messagesRejection = new AtomicLong();
//
//    private final AtomicLong submittedTasks = new AtomicLong();
//    private final AtomicLong completedTasks = new AtomicLong();
//    private final AtomicLong failedTasks = new AtomicLong();
//    private final AtomicLong rejectedTasks = new AtomicLong();
//
//    private final AtomicInteger activeTasks = new AtomicInteger();
//    private final AtomicInteger outstandingTasks = new AtomicInteger();
//
//    private long lastReportedFailedTasks;
//    private long lastReportedRejectedTasks;
//    private long lastReportedAcceptFailures;
//    private long lastReportedDispatchRejections;
//
//    private static final class PeerRetryState {
//
//        private int consecutiveFailures;
//
//        private long nextAttemptNanos;
//
//        private synchronized boolean isDue() {
//
//            return System.nanoTime() >= nextAttemptNanos;
//        }
//
//        private synchronized long recordFailure() {
//
//            consecutiveFailures++;
//
//            int exponent = Math.min(
//                    consecutiveFailures - 1,
//                    10
//            );
//
//            long delaySeconds = Math.min(
//                    MAX_PEER_RETRY_SECONDS,
//                    INITIAL_PEER_RETRY_SECONDS << exponent
//            );
//
//            nextAttemptNanos = System.nanoTime()
//                    + TimeUnit.SECONDS.toNanos(delaySeconds);
//
//            return delaySeconds;
//        }
//    }
//
//    public TCPService(
//            int coreThreads,
//            int maxThreads,
//            long idleTimeout,
//            int tcpPort,
//            int tcpBacklog,
//            String processName,
//            List<TCPPeer> peersToConnect,
//            TCPApplicationProcessor tcpApplicationProcessor,
//            TCPConnectionMode connectionMode,
//            TCPControlProcessor tcpControlProcessor
//    ) {
//        this.coreThreads            = coreThreads;
//        this.maxThreads             = maxThreads;
//        this.idleTimeout            = idleTimeout;
//        this.tcpPort                = tcpPort;
//        this.tcpBacklog             = tcpBacklog;
//        this.processName            = processName;
//
//        this.peersToConnect         = peersToConnect == null
//                ? List.of()
//                : List.copyOf(peersToConnect);
//        this.tcpApplicationProcessor = tcpApplicationProcessor;
//        this.connectionMode         = java.util.Objects.requireNonNull(connectionMode);
//
//        this.isQuiescing            = true;
//
//        this.admissionLock          = new Object();
//        this.activeConnections      = ConcurrentHashMap.newKeySet();
//        this.peerConnections        = new ConcurrentHashMap<>();
//        this.tcpControlProcessor    = tcpControlProcessor;
//    }
//
//
//    public TCPService(
//            int coreThreads,
//            int maxThreads,
//            long idleTimeout,
//            int tcpPort,
//            String processName,
//            List<TCPPeer> peersToConnect,
//            TCPConnectionMode connectionMode,
//            TCPControlProcessor tcpControlProcessor
//    ) {
//        this.coreThreads            = coreThreads;
//        this.maxThreads             = maxThreads;
//        this.idleTimeout            = idleTimeout;
//        this.tcpPort                = tcpPort;
//        this.processName            = processName;
//
//        this.peersToConnect         = peersToConnect == null
//                ? List.of()
//                : List.copyOf(peersToConnect);
//        this.connectionMode         = java.util.Objects.requireNonNull(connectionMode);
//
//        this.isQuiescing            = true;
//
//        this.admissionLock          = new Object();
//        this.activeConnections      = ConcurrentHashMap.newKeySet();
//        this.peerConnections        = new ConcurrentHashMap<>();
//        this.tcpControlProcessor    = tcpControlProcessor;
//
//        // these wont be used in this mode (client_only)
//        tcpBacklog                  = 50;
//
//        // these wont be used in server only and client only modes
//        tcpApplicationProcessor = null;
//    }
//
//    private boolean requiresServerSocket() {
//        return connectionMode != TCPConnectionMode.CLIENT_ONLY;
//    }
//
//    private boolean connectsToPeers() {
//        return connectionMode != TCPConnectionMode.SERVER_ONLY;
//    }
//
//    @Override
//    public void start() {
//
//        tcpWorkers = new ThreadPool().initializeAndGetThreadPool(
//                coreThreads,
//                maxThreads,
//                idleTimeout,
//                false
//        );
//
//        try {
//            if (requiresServerSocket()) {
//                serverSocket = new ServerSocket(
//                        tcpPort,
//                        tcpBacklog,
//                        InetAddress.getLoopbackAddress()
//                );
//            }
//
//            isQuiescing = false;
//
//            if (requiresServerSocket()) {
//                listenerThread = new Thread(
//                        this::listenForConnections,
//                        "tcp-listener"
//                );
//                listenerThread.start();
//            }
//
////            if (connectsToPeers() && !peersToConnect.isEmpty()) {
////
////                startPeerConnectionMonitor();
////            }
//
//            backgroundJobScheduler = new ScheduledThreadPoolExecutor(1);
//
//            backgroundJobScheduler.scheduleAtFixedRate(
//                    this::cleanup,
//                    60,
//                    20,
//                    TimeUnit.SECONDS
//            );
//
//            if (connectsToPeers() && !peersToConnect.isEmpty()) {
//                backgroundJobScheduler.scheduleAtFixedRate(
//                        this::triggerPeerMonitor,
//                        60,
//                        20,
//                        TimeUnit.SECONDS
//                );
//            }
//
//        } catch (IOException exception) {
//            tcpWorkers.shutdownNow();
//            throw new RuntimeException(
//                    "Failed to start TCP service",
//                    exception
//            );
//        }
//
//    }
//
//    private void startPeerConnectionMonitor() {
//
//        synchronized (peerSchedulerLock) {
//
//            if (peerConnectionScheduler != null
//                    && !peerConnectionScheduler.isShutdown()) {
//
//                return;
//            }
//
//            ScheduledThreadPoolExecutor scheduler =
//                    new ScheduledThreadPoolExecutor(
//                            1,
//                            runnable -> {
//
//                                Thread thread = new Thread(
//                                        runnable,
//                                        "tcp-peer-reconciler"
//                                );
//
//                                thread.setDaemon(true);
//
//                                return thread;
//                            }
//                    );
//
//            scheduler.setRemoveOnCancelPolicy(true);
//
//            peerConnectionScheduler = scheduler;
//
//            scheduler.scheduleWithFixedDelay(
//                    this::reconcilePeerConnections,
//                    0,
//                    PEER_RECONCILIATION_INTERVAL_SECONDS,
//                    TimeUnit.SECONDS
//            );
//
//            FileLogger.log(
//                    "[TCPService] Peer connection reconciliation started."
//            );
//        }
//    }
//
//    private void reconcilePeerConnections() {
//
//        if (isQuiescing) {
//            return;
//        }
//
//        for (TCPPeer peer : peersToConnect) {
//
//            if (isQuiescing) {
//                break;
//            }
//
//            String peerId = peer.processId();
//
//            if (peerId == null || peerId.isBlank()) {
//
//                FileLogger.log(
//                        "[TCPService] Peer has an invalid process ID."
//                );
//
//                continue;
//            }
//
//            if (peerConnections.containsKey(peerId)) {
//
//                peerRetryStates.remove(peerId);
//
//                continue;
//            }
//
//
//            PeerRetryState retryState = peerRetryStates.computeIfAbsent(
//                    peerId,
//                    ignored -> new PeerRetryState()
//            );
//
//            if (!retryState.isDue()) {
//                continue;
//            }
//
//            if (!peersConnecting.add(peerId)) {
//                continue;
//            }
//
//            try {
//
//                if (isQuiescing) {
//                    break;
//                }
//
//                FileLogger.log(
//                        "[TCPService] Attempting connection to peer: "
//                                + peerId
//                );
//
//                establishPeerConnection(peer);
//
//                if (peerConnections.containsKey(peerId)) {
//
//                    peerRetryStates.remove(peerId);
//
//                    FileLogger.log(
//                            "[TCPService] Peer connection is active: "
//                                    + peerId
//                    );
//
//                } else {
//
//                    long retryDelay = retryState.recordFailure();
//
//                    FileLogger.log(
//                            "[TCPService] Peer unavailable: "
//                                    + peerId
//                                    + ". Next retry in "
//                                    + retryDelay
//                                    + " seconds."
//                    );
//                }
//
//            } catch (RuntimeException exception) {
//
//                long retryDelay = retryState.recordFailure();
//
//                FileLogger.log(
//                        "[TCPService] Peer reconciliation failed for "
//                                + peerId
//                                + ": "
//                                + exception
//                                + ". Next retry in "
//                                + retryDelay
//                                + " seconds."
//                );
//
//            } finally {
//
//                peersConnecting.remove(peerId);
//            }
//        }
//    }
//
//    private void stopPeerConnectionMonitor() {
//
//        synchronized (peerSchedulerLock) {
//
//            ScheduledExecutorService scheduler =
//                    peerConnectionScheduler;
//
//            peerConnectionScheduler = null;
//
//            if (scheduler != null) {
//
//                scheduler.shutdownNow();
//
//                try {
//
//                    if (!scheduler.awaitTermination(
//                            5,
//                            TimeUnit.SECONDS
//                    )) {
//
//                        FileLogger.log(
//                                "[TCPService] Peer reconciler did not terminate "
//                                        + "within the shutdown deadline."
//                        );
//                    }
//
//                } catch (InterruptedException exception) {
//
//                    Thread.currentThread().interrupt();
//
//                    FileLogger.log(
//                            "[TCPService] Interrupted while stopping peer reconciler."
//                    );
//                }
//            }
//
//            peerRetryStates.clear();
//
//            peersConnecting.clear();
//        }
//    }
//
//    private void listenForConnections() {
//
//        while (!isQuiescing) {
//            Socket clientSocket = null;
//            try {
//                clientSocket = serverSocket.accept();
//                acceptedConnections.incrementAndGet();
//                clientSocket.setKeepAlive(true);
//
//                synchronized (admissionLock) {
//                    if (isQuiescing) {
//                        closeQuietly(clientSocket);
//                        break;
//                    }
//                    activeConnections.add(clientSocket);
//                    Socket acceptedSocket = clientSocket;
//                    try {
//                        submitToThreadPool(
//                                () -> acceptClient(acceptedSocket)
//                        );
//                    } catch (RejectedExecutionException exception) {
//                        connectionTaskRejections.incrementAndGet();
//                        activeConnections.remove(acceptedSocket);
//                        closeQuietly(acceptedSocket);
//                        FileLogger.log(
//                                "[TCPService] Connection rejected: "
//                                        + exception.getMessage()
//                        );
//                    }
//                }
//
//            } catch (IOException exception) {
//                if (!isQuiescing) {
//                    acceptFailures.incrementAndGet();
//                    FileLogger.log(
//                            "[TCPService] Accept failed: "
//                                    + exception.getMessage()
//                    );
//                }
//                break;
//            }
//        }
//    }
//
//    private void initializePeerConnections() {
//        for (TCPPeer peer : peersToConnect) {
//            if (isQuiescing) {
//                break;
//            }
//            establishPeerConnection(peer);
//        }
//    }
//
//    private void establishPeerConnection(TCPPeer peer) {
//        Socket socket = new Socket();
//        ClientConnection clientConnection = null;
//        boolean connectionEstablished = false;
//        try {
//            if (isQuiescing) {
//                return;
//            }
//            socket.connect(
//                    new InetSocketAddress(peer.host(), peer.port()),
//                    CONNECT_TIMEOUT_MILLIS
//            );
//            socket.setKeepAlive(true);
//            clientConnection = new ClientConnection(socket);
//            synchronized (admissionLock) {
//                if (isQuiescing) {
//                    throw new IOException(
//                            "TCP service is shutting down"
//                    );
//                }
//                ClientConnection existingConnection =
//                        peerConnections.putIfAbsent(
//                                peer.processId(),
//                                clientConnection
//                        );
//                if (existingConnection != null) {
//                    throw new IOException(
//                            "Connection already exists for peer: "
//                                    + peer.processId()
//                    );
//                }
//                activeConnections.add(socket);
//                ClientConnection establishedConnection = clientConnection;
//                submitToThreadPool(() -> {
//                    try {
//                        handleClient(establishedConnection);
//                    } finally {
//                        peerConnections.remove(
//                                peer.processId(),
//                                establishedConnection
//                        );
//                        activeConnections.remove(socket);
//                        closeQuietly(socket);
//                    }
//                });
//                connectionEstablished = true;
//            }
//            FileLogger.log(
//                    "[TCPService] Successfully established connection to peer: "
//                            + peer.processId()
//            );
//        } catch (IOException | RejectedExecutionException exception) {
//
//            if (clientConnection != null) {
//                peerConnections.remove(
//                        peer.processId(),
//                        clientConnection
//                );
//            }
//
//            activeConnections.remove(socket);
//            FileLogger.log(
//                    "[TCPService] Failed to establish connection to "
//                            + peer.processId() + ": "
//                            + exception.getMessage()
//            );
//        } finally {
//            if (!connectionEstablished) {
//                closeQuietly(socket);
//            }
//        }
//    }
//
//    private void submitToThreadPool(Runnable task) {
//        try {
//            outstandingTasks.incrementAndGet();
//            tcpWorkers.submit(() -> {
//                activeTasks.incrementAndGet();
//                try {
//                    task.run();
//                } catch (Exception exception) {
//                    failedTasks.incrementAndGet();
//                    throw exception;
//                } finally {
//                    completedTasks.incrementAndGet();
//                    activeTasks.decrementAndGet();
//                    outstandingTasks.decrementAndGet();
//                }
//            });
//            submittedTasks.incrementAndGet();
//        } catch (RejectedExecutionException exception) {
//            outstandingTasks.decrementAndGet();
//            rejectedTasks.incrementAndGet();
//            throw exception;
//        }
//    }
//
//    private void acceptClient(Socket clientSocket) {
//        try {
//            ClientConnection connection =
//                    new ClientConnection(clientSocket);
//            handleClient(connection);
//        } catch (IOException exception) {
//            connectionInitializationFailures.incrementAndGet();
//            FileLogger.log(
//                    "[TCPService] [acceptClient] Client initialization failed: "
//                            + exception.getMessage()
//            );
//            closeQuietly(clientSocket);
//        } finally {
//            activeConnections.remove(clientSocket);
//        }
//    }
//
//    private void handleClient(ClientConnection clientConnection) {
//        try (
//                Socket socket = clientConnection.getSocket();
//                BufferedReader reader = new BufferedReader(
//                        new InputStreamReader(
//                                socket.getInputStream(),
//                                StandardCharsets.UTF_8
//                        )
//                )
//        ) {
//            String rawMessage;
//            while (!isQuiescing && (rawMessage = reader.readLine()) != null) {
//                messagesReceived.incrementAndGet();
//                try {
//                    dispatchIncoming(
//                            rawMessage,
//                            clientConnection
//                    );
//                    messagesSubmitted.incrementAndGet();
//                } catch (RejectedExecutionException exception) {
//                    messagesRejection.incrementAndGet();
//                    FileLogger.log(
//                            "[TCPService] Message submission rejected: "
//                                    + exception.getMessage()
//                    );
//                } catch (Exception exception) {
//                    FileLogger.log(
//                            "[TCPService] Failed to process incoming message: "
//                                    + exception.getMessage()
//                    );
//                }
//            }
//        } catch (IOException exception) {
//            clientReadFailures.incrementAndGet();
//            if (!isQuiescing) {
//                FileLogger.log(
//                        "[TCPService] [handleClient] Client read failed: "
//                                + exception.getMessage()
//                );
//            }
//        }
//    }
//
//    // routes incoming messages to appropriate processors on the basis of tcp mode
//    private void dispatchIncoming(
//            String rawMessage,
//            ClientConnection clientConnection
//    ) throws Exception {
//
//        switch (connectionMode) {
//
//            case SERVER_AND_PEERS -> {
//
//                if (tcpApplicationProcessor == null) {
//
//                    throw new IllegalStateException(
//                            "TCPProcessor is required for SERVER_AND_PEERS mode"
//                    );
//                }
//
//                tcpApplicationProcessor.processIncoming(
//                        rawMessage,
//                        request -> new TCPReplyChannel(
//                                clientConnection,
//                                this
//                        )
//                );
//            }
//
//            case SERVER_ONLY, CLIENT_ONLY -> {
//
//                if (tcpControlProcessor == null) {
//
//                    throw new IllegalStateException(
//                            "TCPControlProcessor is required for control mode"
//                    );
//                }
//
//                tcpControlProcessor.processIncoming(
//                        rawMessage,
//                        clientConnection,
//                        message -> new TCPReplyChannel(
//                                clientConnection,
//                                this
//                        )
//                );
//            }
//        }
//    }
//
//
//    // sends the message to the client using the client connection
//    public CompletableFuture<Void> writeMessage(
//            Message message,
//            ClientConnection clientConnection
//    ) {
//        CompletableFuture<Void> completion = new CompletableFuture<>();
//        try {
//            submitToThreadPool(() -> {
//                try {
//                    tcpApplicationProcessor.processOutgoing(
//                            message,
//                            clientConnection
//                    );
//                    messagesSent.incrementAndGet();
//                    completion.complete(null);
//                } catch (Exception exception) {
//                    messagesFailures.incrementAndGet();
//                    completion.completeExceptionally(exception);
//                    FileLogger.log(
//                            "[TCPService] Message write failed: "
//                                    + exception.getMessage()
//                    );
//                }
//            });
//            messagesSubmitted.incrementAndGet();
//        } catch (RejectedExecutionException exception) {
//            messagesRejection.incrementAndGet();
//            completion.completeExceptionally(exception);
//        }
//        return completion;
//    }
//
//    // stop accepting new work (new connections, or messages FROM CLIENTS over existing connections)
//    // the workers should still accept responses from already received requests
//    @Override
//    public void quiesce() {
//        synchronized (admissionLock) {
//            if (isQuiescing) {
//                return;
//            }
//            isQuiescing = true;
//            if (tcpControlProcessor != null)
//                tcpControlProcessor.failAllPendingRequests(
//                        new IllegalStateException(
//                                "TCP Service is quiescing."
//                        )
//                );
//            stopPeerConnectionMonitor();
//            closeQuietly(serverSocket);
//        }
//    }
//
//    // allows the service to drain all the pending request for some time.
//    @Override
//    public void drain(long timeout) {
//        // stop the listener from accepting new connections
//        if (listenerThread != null) {
//            try {
//                listenerThread.join(timeout);
//            } catch (InterruptedException exception) {
//                Thread.currentThread().interrupt();
//                return;
//            }
//        }
//        // check for work completion for a certain period
//        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
//        while (hasOutstandingWork()) {
//            if (System.nanoTime() >= deadline) {
//                break;
//            }
//            LockSupport.parkNanos(
//                    TimeUnit.MILLISECONDS.toNanos(100)
//            );
//        }
//    }
//
//    private void cleanupInactiveSockets() {
//        for (Socket socket: activeConnections) {
//            if (socket.isClosed()) {
//                closeQuietly(socket);
//                activeConnections.remove(
//                        socket
//                );
//            }
//        }
//
//        for (Map.Entry<String, ClientConnection> connection: peerConnections.entrySet()) {
//            Socket socket = connection.getValue().getSocket();
//            if (socket.isClosed()) {
//                closeQuietly(socket);
//                tcpControlProcessor.removePendingRequestsForConnection(
//                        connection.getValue()
//                );
//                peerConnections.remove(
//                        connection.getKey()
//                );
//            }
//        }
//    }
//
//    private void cleanupCorrelations() {
//        tcpControlProcessor.processAndRemoveExpiredCorrelations();
//    }
//
//    @Override
//    public void cleanup() {
//        try {
//            submitToThreadPool(
//                    this::cleanupCorrelations
//            );
//        } catch (Exception exception) {
//            FileLogger.log(
//                    "[TCPService] [cleanup] Unable to submit cleanupCorrelations task" + exception.getMessage()
//            );
//        }
//        try {
//            submitToThreadPool(
//                    this::cleanupInactiveSockets
//            );
//        } catch (Exception exception) {
//            FileLogger.log(
//                    "[TCPService] [cleanup] Unable to submit cleanupCorrelations task" + exception.getMessage()
//            );
//        }
//    }
//
//    private void reconcilePeers() {
//        if (peersToConnect.size() == peerConnections.size())
//            return;
//        for (TCPPeer tcpPeer: peersToConnect) {
//
//            if (peerConnections.get(tcpPeer.processId()) != null)
//                continue;
//
//            establishPeerConnection(tcpPeer);
//
//            if (peerConnections.containsKey(tcpPeer.processId())) {
//                FileLogger.log(
//                        "[TCPService] Peer connection is active: "
//                                + tcpPeer.processId()
//                );
//            } else {
//
//                FileLogger.log(
//                        "[TCPService] Peer unavailable: "
//                                + tcpPeer.processId()
//                );
//            }
//        }
//    }
//
//    private void triggerPeerMonitor() {
//        submitToThreadPool(
//                this::reconcilePeers
//        );
//    }
//
//    // checks for outstanding requests, if something is pending
//    private boolean hasOutstandingWork() {
////        return outstandingTasks.get() > 0;
//        return true;
//    }
//
//    @Override
//    public void shutdown() {
//        stopPeerConnectionMonitor();
//        for (Socket socket : activeConnections) {
//            closeQuietly(socket);
//        }
//        peerConnections.clear();
//        tcpWorkers.shutdownNow();
//    }
//
//    @Override
//    public synchronized HealthReport checkHealth() {
//
//        Map<String, Object> metrics = new LinkedHashMap<>();
//
//        metrics.put("isQuiescing", isQuiescing);
//        metrics.put("connectionMode", connectionMode.name());
//        metrics.put("serverSocketRequired", requiresServerSocket());
//        metrics.put("peerConnectionsEnabled", connectsToPeers());
//        metrics.put("configuredPeers", peersToConnect.size());
//
//        metrics.put("tcpPort", tcpPort);
//        metrics.put("tcpBacklog", tcpBacklog);
//
//        metrics.put("serverSocketInitialized", serverSocket != null);
//        metrics.put("serverSocketBound",
//                serverSocket != null && serverSocket.isBound());
//        metrics.put("serverSocketClosed",
//                serverSocket == null || serverSocket.isClosed());
//
//        metrics.put("listenerInitialized", listenerThread != null);
//        metrics.put("listenerAlive",
//                listenerThread != null && listenerThread.isAlive());
//
//        metrics.put("executorInitialized", tcpWorkers != null);
//
//        metrics.put("executorShutdown",
//                tcpWorkers != null && tcpWorkers.isShutdown());
//        metrics.put("executorTerminated",
//                tcpWorkers != null && tcpWorkers.isTerminated());
//
//        metrics.put("acceptedConnections", acceptedConnections.get());
//        metrics.put("activeConnections", activeConnections.size());
//
//        metrics.put("connectionInitializationFailures",
//                connectionInitializationFailures.get());
//        metrics.put("connectionTaskRejections",
//                connectionTaskRejections.get());
//        metrics.put("clientReadFailures",
//                clientReadFailures.get());
//        metrics.put("acceptFailures", acceptFailures.get());
//
//        metrics.put("messagesSubmitted", messagesSubmitted.get());
//        metrics.put("messagesSent", messagesSent.get());
//        metrics.put("messagesFailures", messagesFailures.get());
//        metrics.put("messagesRejection", messagesRejection.get());
//
//        metrics.put("submittedTasks", submittedTasks.get());
//        metrics.put("completedTasks", completedTasks.get());
//        metrics.put("failedTasks", failedTasks.get());
//        metrics.put("rejectedTasks", rejectedTasks.get());
//
//        metrics.put("activeTasks", activeTasks.get());
//        metrics.put("outstandingTasks", outstandingTasks.get());
//
//        HealthStatus status;
//        String message;
//
//        if (tcpWorkers == null ||
//                (requiresServerSocket() && serverSocket == null)) {
//            status = HealthStatus.UNREACHABLE;
//            message = "TCP service has not been initialized.";
//
//        } else if (tcpWorkers.isTerminated() || tcpWorkers.isShutdown()) {
//            status = HealthStatus.UNHEALTHY;
//            message = "TCP worker executor is no longer operational.";
//
//        } else if (!isQuiescing && requiresServerSocket() &&
//                (serverSocket.isClosed() ||
//                        listenerThread == null ||
//                        !listenerThread.isAlive())) {
//            status = HealthStatus.UNHEALTHY;
//            message = "TCP listener is no longer accepting connections.";
//
//        } else {
//
//            boolean newTaskFailures =
//                    failedTasks.get() > lastReportedFailedTasks;
//
//            boolean newTaskRejections =
//                    rejectedTasks.get() > lastReportedRejectedTasks;
//
//            boolean newAcceptFailures =
//                    acceptFailures.get() > lastReportedAcceptFailures;
//
//            lastReportedFailedTasks = failedTasks.get();
//            lastReportedRejectedTasks = rejectedTasks.get();
//            lastReportedAcceptFailures = acceptFailures.get();
//
//            if (newTaskFailures ||
//                    newTaskRejections ||
//                    newAcceptFailures) {
//
//                status = HealthStatus.DEGRADED;
//                message = "TCP service is operational but has experienced task failures, connection acceptance failures or message dispatch rejections.";
//
//            } else {
//                status = HealthStatus.HEALTHY;
//                message = "TCP service is operating normally.";
//            }
//        }
//
//        return new HealthReport(
//                "TCPService",
//                status,
//                message,
//                Instant.now(),
//                metrics
//        );
//    }
//
//    private void closeQuietly(AutoCloseable resource) {
//        if (resource == null) {
//            return;
//        }
//        try {
//            resource.close();
//        } catch (Exception exception) {
//            FileLogger.log(
//                    "[TCPService] [closeQuietly] Unable to close socket: " + exception
//            );
//        }
//    }
//
//    private CompletableFuture<Void> sendRequestMessage(Message message) {
//        CompletableFuture<Void> completion = new CompletableFuture<>();
//        try {
//            submitToThreadPool(() -> {
//                try {
//                    String recipientId = message.getRecipientID();
//                    if (recipientId == null || recipientId.isBlank()) {
//                        throw new IllegalArgumentException(
//                                "Recipient ID is required for TCP submission"
//                        );
//                    }
//                    ClientConnection clientConnection =
//                            peerConnections.get(recipientId);
//                    if (clientConnection == null) {
//                        throw new IOException(
//                                "No active connection found for peer: " + recipientId
//                        );
//                    }
//                    tcpApplicationProcessor.processOutgoing(
//                            message,
//                            clientConnection
//                    );
//                    completion.complete(null);
//                } catch (Exception exception) {
//                    completion.completeExceptionally(exception);
//                }
//            });
//        } catch (RejectedExecutionException exception) {
//            completion.completeExceptionally(exception);
//        }
//        return completion;
//    }
//
//    private Message createTransportFailureResponse(
//            Message request,
//            Throwable exception) {
//
//        ObjectNode payload = JsonNodeFactory.instance.objectNode();
//
//        payload.put("errorCode", "TCP_DELIVERY_FAILED");
//        payload.put(
//                "error",
//                exception.getMessage() != null
//                        ? exception.getMessage()
//                        : exception.getClass().getSimpleName()
//        );
//
//        Message response = new Message(
//                MessageType.RESPONSE,
//                request.getProcessType(),
//                request.getOperation(),
//                payload,
//                request.getFullStringOriginalMessage(),
//                null,
//                request.getCorrelationId()
//        );
//
//        response.setStatus("FAILED");
//
//        return response;
//    }
//
//    private void handleSubmissionFailure(
//            Message message,
//            Throwable exception
//    ) {
//
//        FileLogger.log(
//                "[TCPService] Failed to send message to peer "
//                        + message.getRecipientID() + ": "
//                        + exception.getMessage()
//        );
//
//        // only outbound requests have a local requester waiting
//        // for the result. so we generate a correlated transport failure
//        // response so MessageService can notify that requester.
//
//        if (message.getMessageType() != MessageType.REQUEST) {
//            return;
//        }
//
//        try {
//            Message failureResponse =
//                    createTransportFailureResponse(
//                            message,
//                            exception
//                    );
//            tcpApplicationProcessor.processTCPRejection(
//                    failureResponse
//            );
//
//        } catch (Exception dispatchException) {
//
//            FileLogger.log(
//                    "[TCPService] Failed to dispatch transport failure: "
//                            + dispatchException.getMessage()
//            );
//        }
//    }
//
//    @Override
//    public void submit(Message message) {
//        ClientConnection clientConnection = null;
//        Throwable submissionFailure = null;
//        synchronized (admissionLock) {
//            if (isQuiescing &&
//                    (message.getMessageType() == MessageType.REQUEST ||
//                            message.getMessageType() == MessageType.INFO)) {
//                rejectedTasks.incrementAndGet();
//                submissionFailure = new RejectedExecutionException(
//                        "TCP Service is quiescing."
//                );
//
//            } else {
//                String recipientID = message.getRecipientID();
//                if (recipientID == null || recipientID.isBlank()) {
//
//                    submissionFailure = new IllegalArgumentException(
//                            "Message recipient ID is missing."
//                    );
//                } else {
//
//                    clientConnection = peerConnections.get(recipientID);
//
//                    if (clientConnection == null) {
//                        submissionFailure = new IllegalStateException(
//                                "No active connection for peer: " + recipientID
//                        );
//                    }
//                }
//            }
//        }
//        if (submissionFailure != null) {
//            handleSubmissionFailure(message, submissionFailure);
//            return;
//        }
//
//        writeMessage(message, clientConnection)
//                .whenComplete((result, exception) -> {
//
//                    if (exception != null) {
//                        handleSubmissionFailure(message, exception);
//                    }
//                });
//    }
//
//    private void removeFromActiveConnection(Socket socket) {
//        activeConnections.remove(socket);
//    }
//
//    private ClientConnection removeFromPeerConnection(String connectionName) {
//        return peerConnections.remove(connectionName);
//    }
//
////    @Override
////    public CompletableFuture<Message> ping(
////            ClientConnection clientConnection,
////            Message request
////    ) {
////        return tcpControlProcessor.sendRequest(request, clientConnection);
////    }
////
////    @Override
////    public void pong(
////            ClientConnection clientConnection,
////            Message response
////    ) {
////        try {
////            tcpControlProcessor.sendResponse(response, clientConnection);
////        } catch (Exception exception) {
////            throw new IllegalStateException(
////                    "Failed to send control response",
////                    exception
////            );
////        }
////    }
//
//    @Override
//    public CompletionStage<Void> sendRequest(Message request) {
//        return null;
//    }
//
//    @Override
//    public void sendResponse(Message response) {
//
//    }
//
//    @Override
//    public void disconnect(String connectionName) {
//        ClientConnection clientConnection = removeFromPeerConnection(connectionName);
//
//        if (clientConnection == null)
//            return;
//
//        tcpControlProcessor.removePendingRequestsForConnection(
//                clientConnection
//        );
//
//        Socket socket = clientConnection.getSocket();
//
//        removeFromActiveConnection(socket);
//        closeQuietly(socket);
//    }
//}
