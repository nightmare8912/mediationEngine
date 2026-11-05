package org.prats.services.messageService;

import org.prats.definitions.*;
import org.prats.inputHandler.processors.InputProcessor;
import org.prats.loggers.ShellLogger;
import org.prats.services.Service;
import org.prats.inputHandler.utils.MessageSender;
import org.prats.services.ServiceRegistry;
import org.prats.threading.ThreadPool;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;


// the message service does not need a acceptWork param to evaluate whether it should accept work or not as it is the last service to go down, there is no thread alive that is going to submit work to it anyways,
// of-course, we can implement the same logic here, but it would be redundant. (with the platform being an exception, and its fine as the platform knows that these services are down, and wont send them commands)
public class MessageService implements Service, MessageSender, HealthCheckable {

    private final ExecutorService threadPool;
//    private final MessageProcessor messageProcessor;
    private final ProcessType process;
    private final MessageSender messageSender;
    private final InputProcessor inputProcessor;
    private final String serviceName;

    private final AtomicInteger activeTasks;
    private final LongAdder submittedTasks;
    private final LongAdder completedTasks;
    private final LongAdder rejectedTasks;
    private final LongAdder failedTasks;

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
                serviceRegistry
        );
//        this.messageProcessor = new MessageProcessor(
//                process,
//                this
//        );
        this.process            = process;
        this.messageSender      = messageSender;
        this.activeTasks        = new AtomicInteger();
        this.submittedTasks     = new LongAdder();
        this.completedTasks     = new LongAdder();
        this.rejectedTasks      = new LongAdder();
        this.failedTasks        = new LongAdder();
        this.threadPool         = new ThreadPool().initializeAndGetThreadPool(
                coreThreads,
                maxThreads,
                timeout,
                false
        );
    }

    public void setServiceRegistry(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    public ProcessType getProcess() {
        return process;
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

    // this is where the message service would take over the message and let the thread which brought this message go back
    // from here it is the responsibility of the message service to handle the message and deliver it to its intended recipient
    @Override
    public void submit(MessageEntity messageEntity) {
        submitToThreadPool(
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
        try {
            if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
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
    public HealthReport checkHealth() {
        ShellLogger.log("Health Check Initiated for: " + this.getClass().getSimpleName());
        Map<String, Object> metrics = new HashMap<>();

        metrics.put("activeTasks", activeTasks.get());
        metrics.put("submittedTasks", submittedTasks.sum());
        metrics.put("completedTasks", completedTasks.sum());
        metrics.put("rejectedTasks", rejectedTasks.sum());
        metrics.put("failedTasks", failedTasks.sum());

        boolean executorRunning = !threadPool.isShutdown()
                && !threadPool.isTerminated();

        HealthStatus status = executorRunning
                ? HealthStatus.HEALTHY
                : HealthStatus.UNHEALTHY;

        metrics.put("executorRunning", executorRunning);

        HealthReport healthReport = new HealthReport(
                getServiceName(),
                status,
                executorRunning
                        ? "Message service operational"
                        : "Message service threads unavailable",
                Instant.now(),
                metrics
        );

        ShellLogger.log("Report: " + healthReport + "\nHealth Check Ended for: " + this.getClass().getSimpleName());

        return healthReport;
    }
}
