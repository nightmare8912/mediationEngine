package org.prats.platform.tcpService;

import org.prats.loggers.ShellLogger;
import org.prats.messageHandler.MessageProcessor;
import org.prats.messageHandler.MessageSender;
import org.prats.messageHandler.MessageSource;
import org.prats.platform.Service;
import org.prats.platform.messageService.MessageService;
import org.prats.threading.ThreadPool;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class TCPService implements Service, MessageSender {

    private final int tcpPort;
    private final int tcpBacklogs;

    private ServerSocket serverSocket;
    private Thread listenerThread;

    private final ExecutorService threadPool;
    private final MessageProcessor messageProcessor;
    private MessageService messageService;
    private final MessageSource process;

    public TCPService (
            int tcpPort,
            int tcpBacklogs,
            int coreThreads,
            int maxThreads,
            int idleTimeout,
            MessageSource process
    ) {
        this.tcpPort            = tcpPort;
        this.tcpBacklogs        = tcpBacklogs;
        this.process            = process;
        messageProcessor        = new MessageProcessor(process);
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
                Socket clientSocket = serverSocket.accept();

                threadPool.submit(() -> acceptClient(clientSocket));

            } catch (IOException ioe) {
                if (!serverSocket.isClosed()) {
                    ioe.printStackTrace();
                }
            }
        }
    }

    private void acceptClient(Socket clientSocket) {

        try {
            ClientConnection connection =
                    new ClientConnection(clientSocket);

            handleClient(connection);

        } catch (IOException ioe) {
            ioe.printStackTrace();

            try {
                clientSocket.close();
            } catch (IOException ignored) {
            }
        }
    }

    public void handleClient(ClientConnection clientConnection) {
        try (
                Socket socket         = clientConnection.getSocket();
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream())
                );
        ) {
            String message;
            while ((message = reader.readLine()) != null) {
                messageService.submitMessage(message, clientConnection);
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    @Override
    public void sendMessage(
            String message,
            ClientConnection clientConnection
    ) {
        try {
            clientConnection.sendMessage(message);
        } catch (IOException ioe) {
            ShellLogger.log(ioe.getMessage());
        }
    }

    @Override
    public void shutdownService() {
        threadPool.shutdown();
    }
}
