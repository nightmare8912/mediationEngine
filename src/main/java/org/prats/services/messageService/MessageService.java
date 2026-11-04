package org.prats.services.messageService;

import org.prats.definitions.ClientConnection;
import org.prats.definitions.MessageEntity;
import org.prats.inputHandler.processors.InputProcessor;
import org.prats.definitions.ProcessType;
import org.prats.services.Service;
import org.prats.inputHandler.utils.MessageSender;
import org.prats.services.ServiceRegistry;
import org.prats.threading.ThreadPool;
import java.util.concurrent.ExecutorService;


public class MessageService implements Service, MessageSender {

    private final ExecutorService threadPool;
//    private final MessageProcessor messageProcessor;
    private final ProcessType process;
    private final MessageSender messageSender;
    private final InputProcessor inputProcessor;
    private final String serviceName;
    private ServiceRegistry serviceRegistry;

    public MessageService(
            int coreThreads,
            int maxThreads,
            long timeout,
            ProcessType process,
            MessageSender messageSender
    ) {
        this.serviceName        = this.getClass().getSimpleName();
        this.inputProcessor     = new InputProcessor(
                process,
                this,
                serviceRegistry
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

    public void setServiceRegistry(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    public ProcessType getProcess() {
        return process;
    }

    // this is where the message service would take over the message and let the thread which brought this message go back
    // from here it is the responsibility of the message service to handle the message and deliver it to its intended recipient
    @Override
    public void submit(MessageEntity messageEntity) {
        threadPool.submit(
                () -> inputProcessor.processInput(messageEntity)
        );
    }

//     this is message service way to send a message, the message sender here is the tcp service which would write the message on the tcp socket
//     its not the message service, who writes, the message service thread is just responsible to hand it over to the tcp service for it to write
//     and the message service thread would return back to its pool
    @Override
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

    @Override
    public String getServiceName() {
        return serviceName;
    }
}
