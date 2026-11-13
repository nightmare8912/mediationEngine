package org.prats.services.tcpService;

import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.ProcessContext;
import org.prats.definitions.enums.HealthStatus;
import org.prats.definitions.interfaces.HealthCheckable;
import org.prats.definitions.interfaces.Service;
import org.prats.definitions.interfaces.TCPProcessor;
import org.prats.definitions.records.HealthReport;
import org.prats.definitions.records.TCPPeer;
import org.prats.loggers.FileLogger;
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
import java.util.concurrent.*;
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

public class NewTCPService implements Service, HealthCheckable {

    private final int coreThreads;
    private final int maxThreads;
    private final long idleTimeout;
    private final int tcpPort;
    private final int tcpBacklog;
    private final boolean hasServerSocket;

    private final TCPProcessor tcpProcessor;

    private final Object admissionLock
            = new Object();
    private final String processName =
            ProcessContext.getProcessName();

    private final Set<Socket> activeConnections =
            ConcurrentHashMap.newKeySet();

    private final Object peerSchedulerLock = new Object();

    private final AtomicLong acceptedConnections = new AtomicLong();
    private final AtomicLong connectionInitializationFailures = new AtomicLong();
    private final AtomicLong connectionTaskRejections = new AtomicLong();
    private final AtomicLong clientReadFailures = new AtomicLong();
    private final AtomicLong acceptFailures = new AtomicLong();

    private final AtomicLong messagesSubmitted = new AtomicLong();
    private final AtomicLong messagesSent = new AtomicLong();
    private final AtomicLong messagesReceived = new AtomicLong();
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

    private ExecutorService tcpWorkers;
    private ScheduledExecutorService backgroundJobScheduler;
    private Thread listenerThread;
    private ServerSocket serverSocket;

    private volatile boolean isQuiescing;

    public NewTCPService(
        int coreThreads,
        int maxThreads,
        long idleTimeout,
        int tcpPort,
        int tcpBacklog,
        boolean hasServerSocket,
        TCPProcessor tcpProcessor
    ) {
        this.isQuiescing            = true;
        this.coreThreads            = coreThreads;
        this.maxThreads             = maxThreads;
        this.idleTimeout            = idleTimeout;
        this.tcpPort                = tcpPort;
        this.tcpBacklog             = tcpBacklog;
        this.hasServerSocket        = hasServerSocket;
        this.tcpProcessor           = tcpProcessor;
    }

    /*
    * TCPService standard functions
    * */

    // the tcp listener thread will be constantly listen on this port, and accepting connections. Once a connection is accepted
    // the listener thread will submit the socket to tcpWorker pool for it to handle and it will return back to listening.

    public void listenOnServerSocket(
    ) {

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
                        FileLogger.log(
                                "[TCPService] Connection rejected: "
                                        + exception.getMessage()
                        );
                    }
                }

            } catch (IOException exception) {
                if (!isQuiescing) {
                    acceptFailures.incrementAndGet();
                    FileLogger.log(
                            "[TCPService] Accept failed: "
                                    + exception.getMessage()
                    );
                }
                break;
            }
        }

    }

    private void acceptClient(Socket clientSocket) {
        try {
            ClientConnection connection =
                    new ClientConnection(clientSocket);
            handleClient(connection);
        } catch (IOException exception) {
            connectionInitializationFailures.incrementAndGet();
            FileLogger.log(
                    "[TCPService] [acceptClient] Client initialization failed: "
                            + exception.getMessage()
            );
            closeQuietly(clientSocket);
        } finally {
            activeConnections.remove(clientSocket);
        }
    }

    // reads incoming messages from one client, a tcp virtual thread gets assigned to it which is responsible to handle this
    // client and all the messages that are associated with it
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
                messagesReceived.incrementAndGet();
                try {
                    tcpProcessor.processIncoming(
                            rawMessage,
                            clientConnection
                    );
                    messagesSubmitted.incrementAndGet();
                } catch (RejectedExecutionException exception) {
                    messagesRejection.incrementAndGet();
                    FileLogger.log(
                            "[TCPService] Message submission rejected: "
                                    + exception.getMessage()
                    );
                } catch (Exception exception) {
                    FileLogger.log(
                            "[TCPService] Failed to process incoming message: "
                                    + exception.getMessage()
                    );
                }
            }
        } catch (IOException exception) {
            clientReadFailures.incrementAndGet();
            if (!isQuiescing) {
                FileLogger.log(
                        "[TCPService] [handleClient] Client read failed: "
                                + exception.getMessage()
                );
            }
        }
    }

    // writes the message on the given connection. the internal call is synchronized so we only have one
    // thread writing on a socket at a time.
    public CompletableFuture<Void> writeMessage(
            String message,
            ClientConnection clientConnection
    ) {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        try {
            submitToThreadPool(() -> {
                try {
                    // writes on the socket directly
                    clientConnection.sendMessage(
                            message
                    );
                    messagesSent.incrementAndGet();
                    completion.complete(null);
                } catch (Exception exception) {
                    messagesFailures.incrementAndGet();
                    completion.completeExceptionally(exception);
                    FileLogger.log(
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

    /*
    * Checks the health of the service, gets executed on the health checker thread
    * */

    @Override
    public HealthReport checkHealth() {
        Map<String, Object> metrics = new LinkedHashMap<>();

        metrics.put("isQuiescing", isQuiescing);
        metrics.put("serverSocketRequired", hasServerSocket);

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

        if (tcpWorkers == null ||
                (hasServerSocket && serverSocket == null)) {
            status = HealthStatus.UNREACHABLE;
            message = "TCP service has not been initialized.";

        } else if (tcpWorkers.isTerminated() || tcpWorkers.isShutdown()) {
            status = HealthStatus.UNHEALTHY;
            message = "TCP worker executor is no longer operational.";

        } else if (!isQuiescing && hasServerSocket &&
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

    // tries to connect to the given tcp peer, on its specified port and host
    public ClientConnection attemptConnection(TCPPeer peer) {

        String peerId = peer.processId();

        if (isQuiescing) {
            return null;
        }

        Socket socket = new Socket();
        ClientConnection connection = null;

        try {

            socket.connect(
                    new InetSocketAddress(
                            peer.host(),
                            peer.port()),
                    5000
            );

            socket.setKeepAlive(true);

            connection = new ClientConnection(socket);

            acceptedConnections.incrementAndGet();
            activeConnections.add(socket);

            ClientConnection finalConnection = connection;
            submitToThreadPool(
                    () -> handleClient(finalConnection)
            );

            FileLogger.log(
                    "[TCPService] Connected to peer: " + peerId
            );

        } catch (Exception exception) {

            closeQuietly(socket);

            FileLogger.log(
                    "[TCPService] Connection failed to "
                            + peerId + ": " + exception.getMessage()
            );

            return null;
        }
        return connection;
    }

    /*
    * All service lifecycle functions below
    * */

    @Override
    public synchronized void start() {


        if (tcpWorkers != null
                || backgroundJobScheduler != null
                || serverSocket != null
                || listenerThread != null) {

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

                newServerSocket = new ServerSocket(
                        tcpPort,
                        tcpBacklog,
                        InetAddress.getLoopbackAddress()
                );
            }


            newTcpWorkers = new ThreadPool().initializeAndGetThreadPool(
                    coreThreads,
                    maxThreads,
                    idleTimeout,
                    false
            );


            newScheduler = new ScheduledThreadPoolExecutor(1);


            tcpWorkers = newTcpWorkers;
            backgroundJobScheduler = newScheduler;
            serverSocket = newServerSocket;

            if (hasServerSocket) {

                ServerSocket listeningSocket = newServerSocket;

                newListener = new Thread(
                        this::listenOnServerSocket,
                        "server-socket-listener"
                );

                listenerThread = newListener;
            }

            isQuiescing = false;


            newScheduler.scheduleWithFixedDelay(
                    this::triggerCleanup,
                    10,
                    15,
                    TimeUnit.SECONDS
            );

            newScheduler.scheduleWithFixedDelay(
                    this::triggerReconcilePeer,
                    10,
                    10,
                    TimeUnit.SECONDS
            );

            if (newListener != null) {
                newListener.start();
            }

            FileLogger.log(
                    "[TCPService][start] TCP service started successfully."
            );

        } catch (Exception exception) {


            isQuiescing = true;


            closeQuietly(newServerSocket);

            if (newListener != null) {
                newListener.interrupt();
            }

            if (newScheduler != null) {
                newScheduler.shutdownNow();
            }

            if (newTcpWorkers != null) {
                newTcpWorkers.shutdownNow();
            }


            boolean interrupted = false;

            try {
                if (newListener != null) {
                    newListener.join(5000);
                }
            } catch (InterruptedException e) {
                interrupted = true;
            }

            try {
                if (newScheduler != null) {
                    newScheduler.awaitTermination(5, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                interrupted = true;
            }

            try {
                if (newTcpWorkers != null) {
                    newTcpWorkers.awaitTermination(5, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                interrupted = true;
            }


            if (newServerSocket == null || newServerSocket.isClosed()) {
                serverSocket = null;
            }

            if (newListener == null || !newListener.isAlive()) {
                listenerThread = null;
            }

            if (newScheduler == null || newScheduler.isTerminated()) {
                backgroundJobScheduler = null;
            }

            if (newTcpWorkers == null || newTcpWorkers.isTerminated()) {
                tcpWorkers = null;
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }

            FileLogger.log(
                    "[TCPService][start] Startup failed. Resources rolled back: "
                            + exception
            );

            throw new IllegalStateException(
                    "Failed to start TCP service.",
                    exception
            );
        }
    }

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

    @Override
    public void quiesce() {
        synchronized (admissionLock) {
            isQuiescing = true;
        }
    }

    @Override
    public void shutdown() {

        isQuiescing = true;

        if (tcpProcessor != null) {
            tcpProcessor.informShutdown();
        }

        closeQuietly(serverSocket);


        if (backgroundJobScheduler != null) {
            backgroundJobScheduler.shutdown();
        }

        for (Socket socket : activeConnections) {
            closeQuietly(socket);
        }

        if (listenerThread != null) {
            listenerThread.interrupt();
        }

        if (tcpWorkers != null) {
            tcpWorkers.shutdown();
        }

        boolean interrupted = false;

        try {

            if (listenerThread != null) {
                listenerThread.join(5000);
            }

            if (backgroundJobScheduler != null &&
                    !backgroundJobScheduler.awaitTermination(5, TimeUnit.SECONDS)) {

                backgroundJobScheduler.shutdownNow();

                backgroundJobScheduler.awaitTermination(5, TimeUnit.SECONDS);
            }

            if (tcpWorkers != null &&
                    !tcpWorkers.awaitTermination(5, TimeUnit.SECONDS)) {

                tcpWorkers.shutdownNow();

                tcpWorkers.awaitTermination(5, TimeUnit.SECONDS);
            }

        } catch (InterruptedException exception) {

            interrupted = true;

            if (backgroundJobScheduler != null) {
                backgroundJobScheduler.shutdownNow();
            }

            if (tcpWorkers != null) {
                tcpWorkers.shutdownNow();
            }
        }

        boolean terminated =
                (listenerThread == null || !listenerThread.isAlive()) &&
                        (backgroundJobScheduler == null || backgroundJobScheduler.isTerminated()) &&
                        (tcpWorkers == null || tcpWorkers.isTerminated());

        if (terminated) {

            activeConnections.clear();
            serverSocket = null;
            listenerThread = null;
            backgroundJobScheduler = null;
            tcpWorkers = null;

            FileLogger.log(
                    "[TCPService] Shutdown completed. All runtime resources cleared."
            );

        } else {

            FileLogger.log(
                    "[TCPService] Shutdown incomplete. Some threads remain active."
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
        try {
            outstandingTasks.incrementAndGet();
            tcpWorkers.submit(() -> {
                activeTasks.incrementAndGet();
                try {
                    task.run();
                } catch (Exception exception) {
                    failedTasks.incrementAndGet();
                    FileLogger.log(
                            "[TCPService] [submitToThreadPool]: " + exception
                    );
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
            FileLogger.log(
                    "[TCPService] [submitToThreadPool]: " + exception
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
            return;
        }
        try {
            resource.close();
        } catch (Exception exception) {
            FileLogger.log(
                    "[TCPService] [closeQuietly] Unable to close socket: " + exception
            );
        }
    }

    /*
    * All background jobs below
    * */

    private void triggerReconcilePeer() {
        submitToThreadPool(
                this::reconcilePeers
        );
    }

    private void reconcilePeers() {
        tcpProcessor.reconcilePeers();
    }

    private void triggerCleanup() {
        submitToThreadPool(
                this::cleanup
        );
    }

    @Override
    public void cleanup() {
        tcpProcessor.cleanupCorrelations();
    }
}
