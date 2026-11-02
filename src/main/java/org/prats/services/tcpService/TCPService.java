package org.prats.services.tcpService;

import org.prats.definitions.ClientConnectionDefinition;
import org.prats.loggers.ShellLogger;
import org.prats.messageHandler.MessageSender;
import org.prats.definitions.ProcessTypeDefinition;
import org.prats.messageHandler.ReplyChannel;
import org.prats.messageHandler.TCPReplyChannel;
import org.prats.services.Service;
import org.prats.services.messageService.MessageService;
import org.prats.threading.ThreadPool;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;

public class TCPService implements Service, MessageSender {

    private final int tcpPort;
    private final int tcpBacklogs;

    private ServerSocket serverSocket;
    private Thread listenerThread;

    private final ExecutorService threadPool;
    private MessageService messageService;
    private final ProcessTypeDefinition process;

    public TCPService (
            int tcpPort,
            int tcpBacklogs,
            int coreThreads,
            int maxThreads,
            int idleTimeout,
            ProcessTypeDefinition process
    ) {
        this.tcpPort            = tcpPort;
        this.tcpBacklogs        = tcpBacklogs;
        this.process            = process;
        threadPool = new ThreadPool().initializeAndGetThreadPool(
                coreThreads,
                maxThreads,
                idleTimeout
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
        while (!serverSocket.isClosed()) {
            try {
                // block the thread until a connection is made
                Socket clientSocket = serverSocket.accept();
                // let a thread from tcp pool handle this connection, and then this thread returns back to listening for more connections
                threadPool.submit(() -> acceptClient(clientSocket));
            } catch (IOException ioe) {
                if (!serverSocket.isClosed()) {
                    ShellLogger.log(ioe.getMessage());
                }
            }
        }
    }

    private void acceptClient(Socket clientSocket) {
        try {
            // initialize client connection which we are going to use throughout the lifecycle of this connection to manage the communication with this client...
            ClientConnectionDefinition connection =
                    new ClientConnectionDefinition(clientSocket);
            handleClient(connection);
        } catch (IOException ioe) {
            ShellLogger.log(ioe.getMessage());
            try {
                clientSocket.close();
            } catch (IOException ignored) {
                // this connection is wasted, should not happen, and we will log it, in future
                ShellLogger.log(ignored.getMessage());
            }
        }
    }

    // the job of tcp thread is to just read the message and not process it end to end, that is the job of message service
    // so it would read it and pass it to the message service for it to handle... however, the tcp thread is stuck here
    // I will still like it to submit message to other threads,even though it effectively does nothing... as we would like segregation of roles
    public void handleClient(ClientConnectionDefinition clientConnectionDefinition) {
        // use the reply channel when responding
        ReplyChannel replyChannel = new TCPReplyChannel(
                clientConnectionDefinition,
                this
        );
        try (
                Socket socket         = clientConnectionDefinition.getSocket();
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(
                                socket.getInputStream(),
                                StandardCharsets.UTF_8
                        )
                );
        ) {
            String message;
            while ((message = reader.readLine()) != null) {
                messageService.submitMessage(message, replyChannel);
            }
        } catch (IOException ioe) {
            ShellLogger.log(ioe.getMessage());
        }
    }

    // submits the message to the tcp pool, so that the calling program thread doesn't need to wait on tcp, that is the job of tcp threads
    // however, we should also be considering the load on tcp threads this creates... but since we upgraded to virtual threads, this should be okay?
    @Override
    public void sendMessage(
            String message,
            ClientConnectionDefinition clientConnectionDefinition
    ) {
        threadPool.submit(() ->
                writeMessageOnSocket(
                        message,
                        clientConnectionDefinition
                )
        );
    }

    // the tcp thread would write the message on the clientSocket so that client can read it, making it private otherwise, other function may accidentally call it and would now start to write message here, which we don't want
    private void writeMessageOnSocket(
            String message,
            ClientConnectionDefinition clientConnectionDefinition
    ) {
        try {
            clientConnectionDefinition.sendMessage(message);
        } catch (IOException ioe) {
            ShellLogger.log(ioe.getMessage());
        }
    }

    @Override
    public void shutdownService() {
        threadPool.shutdown();
    }
}
