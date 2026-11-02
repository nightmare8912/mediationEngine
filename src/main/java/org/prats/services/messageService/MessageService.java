package org.prats.services.messageService;

import org.prats.inputHandler.InputProcessor;
import org.prats.messageHandler.MessageProcessor;
import org.prats.definitions.ProcessType;
import org.prats.messageHandler.ReplyChannel;
import org.prats.services.Service;
import org.prats.definitions.ClientConnection;
import org.prats.messageHandler.MessageSender;
import org.prats.threading.ThreadPool;
import java.util.concurrent.ExecutorService;


public class MessageService implements Service {

    private final ExecutorService threadPool;
//    private final MessageProcessor messageProcessor;
    private final ProcessType process;
    private final MessageSender messageSender;
    private final InputProcessor inputProcessor;

    public MessageService(
            int coreThreads,
            int maxThreads,
            long timeout,
            ProcessType process,
            MessageSender messageSender
    ) {
        this.inputProcessor   = new InputProcessor(
                process,
                this
        );
//        this.messageProcessor = new MessageProcessor(
//                process,
//                this
//        );
        this.process          = process;
        this.messageSender    = messageSender;
        this.threadPool       = new ThreadPool().initializeAndGetThreadPool(
                coreThreads,
                maxThreads,
                timeout
        );
    }

    public ProcessType getProcess() {
        return process;
    }

    // this is where the message service would take over the message and let the thread which brought this message go back
    // from here it is the responsibility of the message service to handle the message and deliver it to its intended recipient
    public void submitMessage(String message, ReplyChannel replyChannel) {
        threadPool.submit(
                () -> inputProcessor.processInput(
                        message,
                        replyChannel
                )
        );
    }

    // this is message service way to send a message, the message sender here is the tcp service which would write the message on the tcp socket
    // its not the message service, who writes, the message service thread is just responsible to hand it over to the tcp service for it to write
    // and the message service thread would return back to its pool
    public void sendMessage(
            String message,
            ClientConnection clientConnection
    ) {
        messageSender.sendMessage(
                message,
                clientConnection
        );
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
