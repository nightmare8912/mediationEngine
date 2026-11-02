package org.prats.platform.messageService;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.prats.messageHandler.MessageProcessor;
import org.prats.messageHandler.MessageSource;
import org.prats.platform.Service;
import org.prats.platform.tcpService.ClientConnection;
import org.prats.messageHandler.MessageSender;
import org.prats.threading.ThreadPool;
import java.util.concurrent.ExecutorService;


public class MessageService implements Service {

    private final ExecutorService threadPool;
    private final MessageProcessor messageProcessor;
    private final MessageSource process;
    private final MessageSender messageSender;

    public MessageService(
            int coreThreads,
            int maxThreads,
            long timeout,
            MessageSource process,
            MessageSender messageSender
    ) {
        this.messageProcessor = new MessageProcessor(process);
        this.process          = process;
        this.messageSender    = messageSender;
        this.threadPool       = new ThreadPool().initializeAndGetThreadPool(
                coreThreads,
                maxThreads,
                timeout
        );
    }

    public void submitMessage(String message, ClientConnection clientConnection) {
        threadPool.submit(
                () -> {
                    try {
                        messageProcessor.processMessage(message, clientConnection);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }

    public void sendMessage(String message, ClientConnection clientConnection) {
        messageSender.sendMessage(message, clientConnection);
    }

    @Override
    public Service startService() {
        return this;
    }

    @Override
    public void shutdownService() {
        threadPool.shutdown();
    }
}
