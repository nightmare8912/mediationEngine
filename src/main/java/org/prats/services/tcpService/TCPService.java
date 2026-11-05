package org.prats.services.tcpService;

import org.prats.definitions.*;
import org.prats.inputHandler.utils.MessageParser;
import org.prats.loggers.ShellLogger;
import org.prats.inputHandler.utils.MessageSender;
import org.prats.inputHandler.utils.replyUtils.ReplyChannel;
import org.prats.inputHandler.utils.replyUtils.TCPReplyChannel;
import org.prats.services.Service;
import org.prats.services.messageService.MessageService;
import org.prats.threading.ThreadPool;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class TCPService implements Service, MessageSender, HealthCheckable {

    private final int tcpPort;
    private final int tcpBacklogs;

    private final AtomicInteger activeTasks;
    private final AtomicInteger activeConnections;
    private final LongAdder totalConnections;
    private final LongAdder submittedTasks;
    private final LongAdder completedTasks;
    private final LongAdder rejectedTasks;
    private final LongAdder failedTasks;
    private final Object admissionLock;

    private ServerSocket serverSocket;
    private Thread listenerThread;

    private final ExecutorService threadPool;
    private MessageService messageService;
    private final String serviceName;
    private final MessageParser messageParser;

    private volatile boolean acceptWork;

    public TCPService (
            int tcpPort,
            int tcpBacklogs,
            int coreThreads,
            int maxThreads,
            int idleTimeout,
            ProcessType processType
    ) {
        this.serviceName        = this.getClass().getSimpleName();
        this.tcpPort            = tcpPort;
        this.tcpBacklogs        = tcpBacklogs;
        this.messageParser      = new MessageParser(processType);
        this.activeTasks        = new AtomicInteger();
        this.activeConnections  = new AtomicInteger();
        this.totalConnections   = new LongAdder();
        this.submittedTasks     = new LongAdder();
        this.completedTasks     = new LongAdder();
        this.rejectedTasks      = new LongAdder();
        this.failedTasks        = new LongAdder();
        this.admissionLock      = new Object();
        this.acceptWork         = false;
        threadPool = new ThreadPool().initializeAndGetThreadPool(
                coreThreads,
                maxThreads,
                idleTimeout,
                false
        );
    }

    public void setMessageService(MessageService messageService) {
        this.messageService = messageService;
    }

    @Override
    public Service startService() {
        try {
                serverSocket = new ServerSocket(
                        tcpPort,
                        tcpBacklogs,
                        InetAddress.getLoopbackAddress()
                );
            acceptWork = true;
            listenerThread = new Thread(
                    this::listenForConnections,
                    "tcp-listener"
            );
            listenerThread.start();
        } catch (IOException ioe) {
            throw new RuntimeException(
                    "Failed to start TCP service", ioe
            );
        }
        return this;
    }

    public void listenForConnections() {
        while (acceptWork && !serverSocket.isClosed()) {
            try {
                // block the thread until a connection is made
                Socket clientSocket = serverSocket.accept();
                synchronized (admissionLock) {
                    if (acceptWork)
                        submitToThreadPool(() -> acceptClient(clientSocket)); // let a thread from tcp pool handle this connection, and then this thread returns back to listening for more connections
                    else {
                        try {
                            clientSocket.close();
                        } catch (IOException ioException) {
                            ShellLogger.log("Failed to close new client socket. The service attempted to close this new connection, as it is no longer accepting work.");
                        }
                    }
                }
            } catch (IOException ioException) {
                if (!serverSocket.isClosed()) {
                    ShellLogger.log(ioException.getMessage());
                }
            }
        }
    }

    private void acceptClient(Socket clientSocket) {
        try {
            // initialize client connection which we are going to use throughout the lifecycle of this
            // connection to manage the communication with this client...
            ClientConnection connection =
                    new ClientConnection(clientSocket);
            activeConnections.incrementAndGet();
            totalConnections.increment();
            handleClient(connection);
        } catch (IOException ioe) {
            ShellLogger.log(ioe.getMessage());
            try {
                clientSocket.close();
            } catch (IOException ioException) {
                // this connection is wasted, should not happen, and we will log it, in future
                ShellLogger.log(ioException.getMessage());
            }
        }
    }

    // the job of tcp thread is to just read the message and not process it end to end, that is the job of message service
    // so it would read it and pass it to the message service for it to handle... however, the tcp thread is stuck here
    // I will still like it to submit message to other threads,even though it effectively does nothing... as we would like segregation of roles
    public void handleClient(ClientConnection clientConnection) {
        // use the reply channel when responding
        ReplyChannel replyChannel = new TCPReplyChannel(
                clientConnection,
                this
        );
        try (
                Socket socket         = clientConnection.getSocket();
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(
                                socket.getInputStream(),
                                StandardCharsets.UTF_8
                        )
                );
        ) {
            String message;
            while (acceptWork && (message = reader.readLine()) != null) {
                messageService.submit(
                        new MessageEntity(
                                messageParser.createMessageFromJSON(message),
                                replyChannel
                        )
                );
            }
            if (acceptWork)
                activeConnections.decrementAndGet();
        } catch (IOException ioe) {
            ShellLogger.log(ioe.getMessage());
        }
    }

    // submits the message to the tcp pool, so that the calling program thread doesn't need to wait on tcp, that is the job of tcp threads
    // however, we should also be considering the load on tcp threads this creates... but since we upgraded to virtual threads, this should be okay?
    @Override
    public void sendMessage(
            String message,
            ClientConnection clientConnection
    ) {
        submitToThreadPool(
                () -> writeMessageOnSocket(message, clientConnection)
        );
    }

    // the tcp thread would write the message on the clientSocket so that client can read it, making it private otherwise, other function
    // may accidentally call it and would now start to write message here, which we don't want
    private void writeMessageOnSocket(
            String message,
            ClientConnection clientConnection
    ) {
        try {
            clientConnection.sendMessage(message);
        } catch (IOException ioe) {
            ShellLogger.log(ioe.getMessage());
        }
    }

    // centralized the submit task to capture diagnostic at one place
    private void submitToThreadPool(Runnable runnable) {
        submittedTasks.increment();
        try {
            threadPool.submit(() -> {
                activeTasks.incrementAndGet();
                try {
                    runnable.run();
                } catch (Exception e) {
                    failedTasks.increment();
                    throw e;
                } finally {
                    activeTasks.decrementAndGet();
                    completedTasks.increment();
                }
            });
        } catch (RejectedExecutionException e) {
            rejectedTasks.increment();
            throw e;
        }
    }

    @Override
    public void shutdownService() {
        // the modification of this variable is to be done by one thread at a time only to prevent inconsistencies
        // adding this fn with all the multithreading madness going on, was a pain and it has become messy
        // i will come back here for refinement.
        synchronized (admissionLock) {
            acceptWork = false;
        }

        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }

            if (listenerThread != null) {
                listenerThread.join(5000);
            }

            threadPool.shutdown();

            if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }

        } catch (IOException e) {
            threadPool.shutdownNow();

        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public void submit(MessageEntity messageEntity) {
        // cannot think of anything for now
    }

    @Override
    public HealthReport checkHealth() {

        Map<String, Object> metrics = new HashMap<>();

        metrics.put("listenerRunning",
                serverSocket != null && !serverSocket.isClosed());

        metrics.put("activeTasks", activeTasks.get());
        metrics.put("submittedTasks", submittedTasks.sum());
        metrics.put("completedTasks", completedTasks.sum());
        metrics.put("rejectedTasks", rejectedTasks.sum());
        metrics.put("failedTasks", failedTasks.sum());

        metrics.put("activeConnections", activeConnections.get());
        metrics.put("totalConnections", totalConnections.sum());

        boolean listenerRunning =
                serverSocket != null && !serverSocket.isClosed();

        boolean executorRunning = !threadPool.isShutdown()
                && !threadPool.isTerminated();

        HealthStatus status = listenerRunning && executorRunning
                ? HealthStatus.HEALTHY
                : HealthStatus.UNHEALTHY;

        metrics.put("listenerRunning", listenerRunning);
        metrics.put("executorRunning", executorRunning);

        return new HealthReport(
                getServiceName(),
                status,
                listenerRunning && executorRunning
                        ? "TCP service operational"
                        : "TCP listener or threads unavailable",
                Instant.now(),
                metrics
        );
    }
}
