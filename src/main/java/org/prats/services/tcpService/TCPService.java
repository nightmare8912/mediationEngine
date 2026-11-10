
package org.prats.services.tcpService;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.prats.definitions.*;
import org.prats.processors.TCPProcessor;
import org.prats.replyChannels.TCPReplyChannel;
import org.prats.loggers.ShellLogger;
import org.prats.services.databaseService.Submittable;
import org.prats.services.MessageDispatcher;
import org.prats.services.Service;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

// listens to incoming tcp requests and forwards it to the message dispatcher
// internal services can also use tcp service to communicate to external jvm's
public class TCPService implements Service, Submittable, HealthCheckable {

    private static final int CONNECT_TIMEOUT_MILLIS = 5000;
    private static final int HANDSHAKE_TIMEOUT_MILLIS = 5000;

    private final TCPProcessor tcpProcessor;
    private final Object admissionLock;

    private final int coreThreads;
    private final int maxThreads;
    private final long idleTimeout;
    private final int tcpPort;
    private final int tcpBacklog;
    private final String processName;

    private final Set<Socket> activeConnections;
    private final Map<String, ClientConnection> peerConnections;
    private final List<TCPPeer> peersToConnect;

    private ExecutorService tcpWorkers;
    private Thread listenerThread;
    private Thread peerConnectorThread;
    private ServerSocket serverSocket;

    private volatile boolean isQuiescing;

    private final AtomicLong acceptedConnections = new AtomicLong();
    private final AtomicLong connectionInitializationFailures = new AtomicLong();
    private final AtomicLong connectionTaskRejections = new AtomicLong();
    private final AtomicLong clientReadFailures = new AtomicLong();
    private final AtomicLong acceptFailures = new AtomicLong();

    private final AtomicLong messagesSubmitted = new AtomicLong();
    private final AtomicLong messagesSent = new AtomicLong();
    private final AtomicLong messagesFailures = new AtomicLong();
    private final AtomicLong messagesRejection = new AtomicLong();

    private final AtomicLong submittedTasks = new AtomicLong();
    private final AtomicLong completedTasks = new AtomicLong();
    private final AtomicLong failedTasks = new AtomicLong();
    private final AtomicLong rejectedTasks = new AtomicLong();

    private final AtomicInteger activeTasks = new AtomicInteger();
    private final AtomicInteger outstandingTasks = new AtomicInteger();

    private long lastReportedFailedTasks;
    private long lastReportedRejectedTasks;
    private long lastReportedAcceptFailures;
    private long lastReportedDispatchRejections;

    public TCPService(
            int coreThreads,
            int maxThreads,
            long idleTimeout,
            int tcpPort,
            int tcpBacklog,
            String processName,
            List<TCPPeer> peersToConnect,
            TCPProcessor tcpProcessor
    ) {
        this.coreThreads        = coreThreads;
        this.maxThreads         = maxThreads;
        this.idleTimeout        = idleTimeout;
        this.tcpPort            = tcpPort;
        this.tcpBacklog         = tcpBacklog;
        this.processName        = processName;

        this.peersToConnect     = peersToConnect;
        this.tcpProcessor       = tcpProcessor;

        this.isQuiescing        = true;

        this.admissionLock      = new Object();
        this.activeConnections  = ConcurrentHashMap.newKeySet();
        this.peerConnections    = new ConcurrentHashMap<>();
    }

    @Override
    public void start() {

        tcpWorkers = new ThreadPool().initializeAndGetThreadPool(
                coreThreads,
                maxThreads,
                idleTimeout,
                false
        );

        try {
            serverSocket = new ServerSocket(
                    tcpPort,
                    tcpBacklog,
                    InetAddress.getLoopbackAddress()
            );

            isQuiescing = false;

            listenerThread = new Thread(
                    this::listenForConnections,
                    "tcp-listener"
            );
            listenerThread.start();

            peerConnectorThread = new Thread(
                    this::initializePeerConnections,
                    "tcp-peer-connector"
            );
            peerConnectorThread.start();
        } catch (IOException exception) {
            tcpWorkers.shutdownNow();
            throw new RuntimeException(
                    "Failed to start TCP service",
                    exception
            );
        }

    }

    private void listenForConnections() {

        while (!isQuiescing) {
            Socket clientSocket = null;
            try {
                clientSocket = serverSocket.accept();
                acceptedConnections.incrementAndGet();
                clientSocket.setKeepAlive(true);

                synchronized (admissionLock) {
                    if (isQuiescing) {
                        closeQuietly(clientSocket);
                        break;
                    }
                    activeConnections.add(clientSocket);
                    Socket acceptedSocket = clientSocket;
                    try {
                        submitToThreadPool(
                                () -> acceptClient(acceptedSocket)
                        );
                    } catch (RejectedExecutionException exception) {
                        connectionTaskRejections.incrementAndGet();
                        activeConnections.remove(acceptedSocket);
                        closeQuietly(acceptedSocket);
                        ShellLogger.log(
                                "[TCPService] Connection rejected: "
                                        + exception.getMessage()
                        );
                    }
                }

            } catch (IOException exception) {
                if (!isQuiescing) {
                    acceptFailures.incrementAndGet();
                    ShellLogger.log(
                            "[TCPService] Accept failed: "
                                    + exception.getMessage()
                    );
                }
                break;
            }
        }
    }

    private void initializePeerConnections() {
        for (TCPPeer peer : peersToConnect) {
            if (isQuiescing) {
                break;
            }
            establishPeerConnection(peer);
        }
    }

    private void establishPeerConnection(TCPPeer peer) {
        Socket socket = new Socket();
        ClientConnection clientConnection = null;
        boolean connectionEstablished = false;
        try {
            if (isQuiescing) {
                return;
            }
            socket.connect(
                    new InetSocketAddress(peer.host(), peer.port()),
                    CONNECT_TIMEOUT_MILLIS
            );
            socket.setKeepAlive(true);
            clientConnection = new ClientConnection(socket);
            synchronized (admissionLock) {
                if (isQuiescing) {
                    throw new IOException(
                            "TCP service is shutting down"
                    );
                }
                ClientConnection existingConnection =
                        peerConnections.putIfAbsent(
                                peer.processId(),
                                clientConnection
                        );
                if (existingConnection != null) {
                    throw new IOException(
                            "Connection already exists for peer: "
                                    + peer.processId()
                    );
                }
                activeConnections.add(socket);
                ClientConnection establishedConnection = clientConnection;
                submitToThreadPool(() -> {
                    try {
                        handleClient(establishedConnection);
                    } finally {
                        peerConnections.remove(
                                peer.processId(),
                                establishedConnection
                        );
                        activeConnections.remove(socket);
                        closeQuietly(socket);
                    }
                });
                connectionEstablished = true;
            }
            ShellLogger.log(
                    "[TCPService] Successfully established connection to peer: "
                            + peer.processId()
            );
        } catch (IOException | RejectedExecutionException exception) {

            if (clientConnection != null) {
                peerConnections.remove(
                        peer.processId(),
                        clientConnection
                );
            }

            activeConnections.remove(socket);
            ShellLogger.log(
                    "[TCPService] Failed to establish connection to "
                            + peer.processId() + ": "
                            + exception.getMessage()
            );
        } finally {

            if (!connectionEstablished) {
                closeQuietly(socket);
            }
        }
    }

    private void submitToThreadPool(Runnable task) {
        try {
            outstandingTasks.incrementAndGet();
            tcpWorkers.submit(() -> {
                activeTasks.incrementAndGet();
                try {
                    task.run();
                } catch (Exception exception) {
                    failedTasks.incrementAndGet();
                    throw exception;
                } finally {
                    completedTasks.incrementAndGet();
                    activeTasks.decrementAndGet();
                    outstandingTasks.decrementAndGet();
                }
            });
            submittedTasks.incrementAndGet();
        } catch (RejectedExecutionException exception) {
            outstandingTasks.decrementAndGet();
            rejectedTasks.incrementAndGet();
            throw exception;
        }
    }

    private void acceptClient(Socket clientSocket) {
        try {
            ClientConnection connection =
                    new ClientConnection(clientSocket);
            handleClient(connection);
        } catch (IOException exception) {
            connectionInitializationFailures.incrementAndGet();
            ShellLogger.log(
                    "[TCPService] [acceptClient] Client initialization failed: "
                            + exception.getMessage()
            );
            closeQuietly(clientSocket);
        } finally {
            activeConnections.remove(clientSocket);
        }
    }

    private void handleClient(ClientConnection clientConnection) {
        try (
                Socket socket = clientConnection.getSocket();
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(
                                socket.getInputStream(),
                                StandardCharsets.UTF_8
                        )
                )
        ) {
            String rawMessage;
            while (!isQuiescing && (rawMessage = reader.readLine()) != null) {
                // messagesReceived.incrementAndGet();
                try {
                    tcpProcessor.processIncoming(
                            rawMessage,
                            request -> new TCPReplyChannel(
                                    clientConnection,
                                    this
                            )
                    );
                    messagesSubmitted.incrementAndGet();
                } catch (RejectedExecutionException exception) {
                    messagesRejection.incrementAndGet();
                    ShellLogger.log(
                            "[TCPService] Message submission rejected: "
                                    + exception.getMessage()
                    );
                } catch (Exception exception) {
                    ShellLogger.log(
                            "[TCPService] Failed to process incoming message: "
                                    + exception.getMessage()
                    );
                }
            }
        } catch (IOException exception) {
            clientReadFailures.incrementAndGet();
            if (!isQuiescing) {
                ShellLogger.log(
                        "[TCPService] [handleClient] Client read failed: "
                                + exception.getMessage()
                );
            }
        }
    }

    // sends the message to the client using the client connection
    public CompletableFuture<Void> writeMessage(
            Message message,
            ClientConnection clientConnection
    ) {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        try {
            submitToThreadPool(() -> {
                try {
                    tcpProcessor.processOutgoing(
                            message,
                            clientConnection
                    );
                    messagesSent.incrementAndGet();
                    completion.complete(null);
                } catch (Exception exception) {
                    messagesFailures.incrementAndGet();
                    completion.completeExceptionally(exception);
                    ShellLogger.log(
                            "[TCPService] Message write failed: "
                                    + exception.getMessage()
                    );
                }
            });
            messagesSubmitted.incrementAndGet();
        } catch (RejectedExecutionException exception) {
            messagesRejection.incrementAndGet();
            completion.completeExceptionally(exception);
        }
        return completion;
    }

    // stop accepting new work (new connections, or messages FROM CLIENTS over existing connections)
    // the workers should still accept responses from already received requests
    @Override
    public void quiesce() {
        synchronized (admissionLock) {
            if (isQuiescing) {
                return;
            }
            isQuiescing = true;
            closeQuietly(serverSocket);
        }
    }

    // allows the service to drain all the pending request for some time.
    @Override
    public void drain(long timeout) {
        // stop the listener from accepting new connections
        if (listenerThread != null) {
            try {
                listenerThread.join(timeout);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        // check for work completion for a certain period
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
        while (hasOutstandingWork()) {
            if (System.nanoTime() >= deadline) {
                break;
            }
            LockSupport.parkNanos(
                    TimeUnit.MILLISECONDS.toNanos(100)
            );
        }
    }

    // checks for outstanding requests, if something is pending
    private boolean hasOutstandingWork() {
//        return outstandingTasks.get() > 0;
        return true;
    }

    @Override
    public void shutdown() {
        for (Socket socket : activeConnections) {
            closeQuietly(socket);
        }
        tcpWorkers.shutdownNow();
    }

    @Override
    public synchronized HealthReport checkHealth() {

        Map<String, Object> metrics = new LinkedHashMap<>();

        metrics.put("isQuiescing", isQuiescing);

        metrics.put("tcpPort", tcpPort);
        metrics.put("tcpBacklog", tcpBacklog);

        metrics.put("serverSocketInitialized", serverSocket != null);
        metrics.put("serverSocketBound",
                serverSocket != null && serverSocket.isBound());
        metrics.put("serverSocketClosed",
                serverSocket == null || serverSocket.isClosed());

        metrics.put("listenerInitialized", listenerThread != null);
        metrics.put("listenerAlive",
                listenerThread != null && listenerThread.isAlive());

        metrics.put("executorInitialized", tcpWorkers != null);

        metrics.put("executorShutdown",
                tcpWorkers != null && tcpWorkers.isShutdown());
        metrics.put("executorTerminated",
                tcpWorkers != null && tcpWorkers.isTerminated());

        metrics.put("acceptedConnections", acceptedConnections.get());
        metrics.put("activeConnections", activeConnections.size());

        metrics.put("connectionInitializationFailures",
                connectionInitializationFailures.get());
        metrics.put("connectionTaskRejections",
                connectionTaskRejections.get());
        metrics.put("clientReadFailures",
                clientReadFailures.get());
        metrics.put("acceptFailures", acceptFailures.get());

        metrics.put("messagesSubmitted", messagesSubmitted.get());
        metrics.put("messagesSent", messagesSent.get());
        metrics.put("messagesFailures", messagesFailures.get());
        metrics.put("messagesRejection", messagesRejection.get());

        metrics.put("submittedTasks", submittedTasks.get());
        metrics.put("completedTasks", completedTasks.get());
        metrics.put("failedTasks", failedTasks.get());
        metrics.put("rejectedTasks", rejectedTasks.get());

        metrics.put("activeTasks", activeTasks.get());
        metrics.put("outstandingTasks", outstandingTasks.get());

        HealthStatus status;
        String message;

        if (tcpWorkers == null || serverSocket == null) {
            status = HealthStatus.UNREACHABLE;
            message = "TCP service has not been initialized.";

        } else if (tcpWorkers.isTerminated() || tcpWorkers.isShutdown()) {
            status = HealthStatus.UNHEALTHY;
            message = "TCP worker executor is no longer operational.";

        } else if (!isQuiescing &&
                (serverSocket.isClosed() ||
                        listenerThread == null ||
                        !listenerThread.isAlive())) {
            status = HealthStatus.UNHEALTHY;
            message = "TCP listener is no longer accepting connections.";

        } else {

            boolean newTaskFailures =
                    failedTasks.get() > lastReportedFailedTasks;

            boolean newTaskRejections =
                    rejectedTasks.get() > lastReportedRejectedTasks;

            boolean newAcceptFailures =
                    acceptFailures.get() > lastReportedAcceptFailures;

            lastReportedFailedTasks = failedTasks.get();
            lastReportedRejectedTasks = rejectedTasks.get();
            lastReportedAcceptFailures = acceptFailures.get();

            if (newTaskFailures ||
                    newTaskRejections ||
                    newAcceptFailures) {

                status = HealthStatus.DEGRADED;
                message = "TCP service is operational but has experienced task failures, connection acceptance failures or message dispatch rejections.";

            } else {
                status = HealthStatus.HEALTHY;
                message = "TCP service is operating normally.";
            }
        }

        return new HealthReport(
                "TCPService",
                status,
                message,
                Instant.now(),
                metrics
        );
    }

    private void closeQuietly(AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception ignored) {
            // try to clean-up, the best we can do now...
        }
    }

    private CompletableFuture<Void> sendRequestMessage(Message message) {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        try {
            submitToThreadPool(() -> {
                try {
                    String recipientId = message.getRecipientID();
                    if (recipientId == null || recipientId.isBlank()) {
                        throw new IllegalArgumentException(
                                "Recipient ID is required for TCP submission"
                        );
                    }
                    ClientConnection clientConnection =
                            peerConnections.get(recipientId);
                    if (clientConnection == null) {
                        throw new IOException(
                                "No active connection found for peer: " + recipientId
                        );
                    }
                    tcpProcessor.processOutgoing(
                            message,
                            clientConnection
                    );
                    completion.complete(null);
                } catch (Exception exception) {
                    completion.completeExceptionally(exception);
                }
            });
        } catch (RejectedExecutionException exception) {
            completion.completeExceptionally(exception);
        }
        return completion;
    }

    private Message createTransportFailureResponse(
            Message request,
            Throwable exception) {

        ObjectNode payload = JsonNodeFactory.instance.objectNode();

        payload.put("errorCode", "TCP_DELIVERY_FAILED");
        payload.put(
                "error",
                exception.getMessage() != null
                        ? exception.getMessage()
                        : exception.getClass().getSimpleName()
        );

        Message response = new Message(
                MessageType.RESPONSE,
                request.getProcessType(),
                request.getOperation(),
                payload,
                request.getFullStringOriginalMessage(),
                null,
                request.getCorrelationId()
        );

        response.setStatus("FAILED");

        return response;
    }

    private void handleSubmissionFailure(
            Message message,
            Throwable exception
    ) {

        ShellLogger.log(
                "[TCPService] Failed to send message to peer "
                        + message.getRecipientID() + ": "
                        + exception.getMessage()
        );

        // only outbound requests have a local requester waiting
        // for the result. so we generate a correlated transport failure
        // response so MessageService can notify that requester.

        if (message.getMessageType() != MessageType.REQUEST) {
            return;
        }

        try {
            Message failureResponse =
                    createTransportFailureResponse(
                            message,
                            exception
                    );
            tcpProcessor.processTCPRejection(
                    failureResponse
            );

        } catch (Exception dispatchException) {

            ShellLogger.log(
                    "[TCPService] Failed to dispatch transport failure: "
                            + dispatchException.getMessage()
            );
        }
    }

    @Override
    public void submit(Message message) {
        ClientConnection clientConnection = null;
        Throwable submissionFailure = null;
        synchronized (admissionLock) {
            if (isQuiescing &&
                    (message.getMessageType() == MessageType.REQUEST ||
                            message.getMessageType() == MessageType.INFO)) {
                rejectedTasks.incrementAndGet();
                submissionFailure = new RejectedExecutionException(
                        "TCP Service is quiescing."
                );

            } else {
                String recipientID = message.getRecipientID();
                if (recipientID == null || recipientID.isBlank()) {

                    submissionFailure = new IllegalArgumentException(
                            "Message recipient ID is missing."
                    );
                } else {

                    clientConnection = peerConnections.get(recipientID);

                    if (clientConnection == null) {
                        submissionFailure = new IllegalStateException(
                                "No active connection for peer: " + recipientID
                        );
                    }
                }
            }
        }
        if (submissionFailure != null) {
            handleSubmissionFailure(message, submissionFailure);
            return;
        }

        writeMessage(message, clientConnection)
                .whenComplete((result, exception) -> {

                    if (exception != null) {
                        handleSubmissionFailure(message, exception);
                    }
                });
    }
}
