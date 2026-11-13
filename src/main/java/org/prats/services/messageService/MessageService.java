
package org.prats.services.messageService;

import org.prats.definitions.classes.Message;
import org.prats.definitions.classes.PendingDelivery;
import org.prats.definitions.enums.HealthStatus;
import org.prats.definitions.interfaces.HealthCheckable;
import org.prats.definitions.records.HealthReport;
import org.prats.loggers.FileLogger;
import org.prats.registries.CorrelationRegistry;
import org.prats.definitions.interfaces.ReplyChannel;
import org.prats.registries.RetryRegistry;
import org.prats.processors.RequestProcessor;
import org.prats.processors.ResponseProcessor;
import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.definitions.interfaces.Service;
import org.prats.threading.ThreadPool;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

// the message service is the heart of all the communication that happens within the container
// majority of the inter-service communication flows through it.
// it implements the message dispatcher interface which other services uses to communicate to each other
// for their req-response cycles. it has several components
// request processor -> to handle incoming requests
// response processor -> to handle incoming responses (incoming for message service, outgoing for the container jvm)
// command processor -> to handle commands, request processor calls command processor if it is not able to decode an incoming request
// the command processor then attempts to decode it creates it in the format that the request processor can understand
// the request processor would route it to the message processor this time which decodes it and understand where it needs to go
// it uses dedicated forwarders which forward the message to their respective service
// the method dispatchRequest and dispatchResponse is where msg worker thread takes over the caller thread
// as the parsing and transport of the message within the container is this service's responsibility
// the request and response processor maintain a correlation registry to keep track of request and responses
// one the response is sent for a request, and it is successful, the correlation registry's entry for that message is removed
// it has a retry registry which logs failure of responses and some background tasks which do this response
// retry and correlation cleaning.

public class MessageService implements Service, MessageDispatcher, HealthCheckable {

    private final Object admissionLock;
    private final CorrelationRegistry correlationRegistry;
    private final RequestProcessor requestProcessor;
    private final ResponseProcessor responseProcessor;
    private final RetryRegistry retryRegistry;

    private ExecutorService messageWorker;
    private ScheduledExecutorService backgroundScheduler;
    private int coreThread;
    private int maxThread;
    private long idleTimeout;
    private long correlationTimeout;

    private volatile boolean isQuiescing;

    private final AtomicLong submittedTasks = new AtomicLong();
    private final AtomicLong completedTasks = new AtomicLong();
    private final AtomicLong failedTasks = new AtomicLong();
    private final AtomicLong rejectedTasks = new AtomicLong();
    private final AtomicLong rejectedDuringQuiesce = new AtomicLong();

    private final AtomicLong submittedRequestTasks = new AtomicLong();
    private final AtomicLong completedRequestTasks = new AtomicLong();

    private final AtomicLong submittedResponseTasks = new AtomicLong();
    private final AtomicLong completedResponseTasks = new AtomicLong();

    private final AtomicLong failedRequestTasks = new AtomicLong();
    private final AtomicLong failedResponseTasks = new AtomicLong();

    private final AtomicLong rejectedRequestTasks = new AtomicLong();
    private final AtomicLong rejectedResponseTasks = new AtomicLong();

    private final AtomicInteger activeTasks = new AtomicInteger();
    private final AtomicInteger outstandingTasks = new AtomicInteger();

    private final AtomicBoolean retryJobQueued;
    private final AtomicBoolean cleanupJobQueued;

    private long lastReportedFailedTasks;
    private long lastReportedRejectedTasks;

    private final long retryInterval;
    private final long cleanupInterval;

    public MessageService(
            int coreThread,
            int maxThread,
            long idleTimeout,
            long retryInterval,
            long cleanupInterval,
            long correlationTimeout,
            RequestProcessor requestProcessor,
            ResponseProcessor responseProcessor,
            CorrelationRegistry correlationRegistry,
            RetryRegistry retryRegistry
    ) {
        this.coreThread             = coreThread;
        this.maxThread              = maxThread;
        this.idleTimeout            = idleTimeout;
        this.isQuiescing            = true;
        this.retryInterval          = retryInterval;
        this.cleanupInterval        = cleanupInterval;
        this.correlationTimeout     = correlationTimeout;

        this.retryJobQueued         = new AtomicBoolean(false);
        this.cleanupJobQueued       = new AtomicBoolean(false);
        this.admissionLock          = new Object();
        this.correlationRegistry    = correlationRegistry;
        this.retryRegistry          = retryRegistry;

        this.requestProcessor       = requestProcessor;
        this.responseProcessor      = responseProcessor;
    }

    @Override
    public void start() {
        messageWorker = new ThreadPool().initializeAndGetThreadPool(
                coreThread,
                maxThread,
                idleTimeout,
                false
        );
        isQuiescing = false;
        startBackgroundJobs();
    }

    private void startBackgroundJobs() {

        backgroundScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "message-service-scheduler");
            thread.setDaemon(true);
            return thread;
        });

        backgroundScheduler.scheduleWithFixedDelay(
                this::triggerRetryJob,
                retryInterval,
                retryInterval,
                TimeUnit.SECONDS
        );

        backgroundScheduler.scheduleWithFixedDelay(
                this::triggerCorrelationCleanupJob,
                cleanupInterval,
                cleanupInterval,
                TimeUnit.SECONDS
        );
    }

    private void triggerRetryJob() {

        if (!retryJobQueued.compareAndSet(false, true)) {
            return;
        }

        try {
            submitToThreadPool(() -> {
                try {
                    retryFailedResponses();
                } finally {
                    retryJobQueued.set(false);
                }
            }, false);

        } catch (RejectedExecutionException exception) {
            retryJobQueued.set(false);
        }
    }


    private void triggerCorrelationCleanupJob() {

        if (!cleanupJobQueued.compareAndSet(false, true)) {
            return;
        }
        try {
            submitToThreadPool(() -> {
                try {
                    try {
                        cleanExpiredCorrelations();
                    } finally {
                        cleanExpiredPendingDeliveries();
                    }
                } finally {
                    cleanupJobQueued.set(false);
                }
            }, false);
        } catch (RejectedExecutionException exception) {

            cleanupJobQueued.set(false);
        }
    }



    private void retryFailedResponses() {
        for (PendingDelivery pendingDelivery :
                retryRegistry.getAllFailures()) {
            responseProcessor.retry(pendingDelivery);
        }
    }


    private void cleanExpiredCorrelations() {
        List<CorrelationRegistry.ExpiredCorrelation> expired =
                correlationRegistry.removeExpired(correlationTimeout);
        for (CorrelationRegistry.ExpiredCorrelation entry : expired) {
            responseProcessor.processExpiry(
                    entry.correlationId(),
                    entry.replyChannel()
            );
        }
    }


    private void cleanExpiredPendingDeliveries() {

        List<PendingDelivery> expiredDeliveries =
                retryRegistry.removeExpired();

        for (PendingDelivery pendingDelivery : expiredDeliveries) {

            FileLogger.log(
                    "Pending response delivery expired: "
                            + pendingDelivery.getCorrelationId()
                            + " | Retry attempts: "
                            + pendingDelivery.getRetryCount()
            );
        }
    }


    @Override
    public void shutdown() {
        if (backgroundScheduler != null) {
            backgroundScheduler.shutdownNow();
        }
        messageWorker.shutdownNow();
    }

    @Override
    public void quiesce() {
        synchronized (admissionLock) {
            isQuiescing = true;
        }
    }

    @Override
    public void drain(long timeout) {
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
    public void cleanup() {

    }

    private boolean hasOutstandingWork() {
        return outstandingTasks.get() > 0 || correlationRegistry.getSize() > 0; // add the retry registry here too in future
    }

    // handled by request processor
    @Override
    public void dispatchRequest(
            Message message,
            ReplyChannel replyChannel
    ) {
        synchronized (admissionLock) {
            if (isQuiescing) {
                rejectedDuringQuiesce.incrementAndGet();
                throw new RejectedExecutionException(
                        "MessageService is quiescing"
                );
            }
            // request forwarded to request processor
            submitToThreadPool(
                    () -> requestProcessor.process(
                            message,
                            replyChannel
                    ),
                    true
            );
        }
    }

    // handled by response processor
    @Override
    public void dispatchResponse(Message message) {
        submitToThreadPool(
                () -> responseProcessor.process(message),
                false
        );
    }

    private void submitToThreadPool(Runnable runnable, boolean requestTask) {
        outstandingTasks.incrementAndGet();
        submittedTasks.incrementAndGet();
        if (requestTask) {
            submittedRequestTasks.incrementAndGet();
        } else {
            submittedResponseTasks.incrementAndGet();
        }
        try {
            messageWorker.submit(() -> {
                activeTasks.incrementAndGet();
                try {
                    runnable.run();
                } catch (Exception e) {
                    failedTasks.incrementAndGet();
                    if (requestTask) {
                        failedRequestTasks.incrementAndGet();
                    } else {
                        failedResponseTasks.incrementAndGet();
                    }
                    throw e;
                } finally {
                    completedTasks.incrementAndGet();
                    activeTasks.decrementAndGet();
                    outstandingTasks.decrementAndGet();
                    if (requestTask) {
                        completedRequestTasks.incrementAndGet();
                    } else {
                        completedResponseTasks.incrementAndGet();
                    }
                }
            });

        } catch (RejectedExecutionException e) {
            outstandingTasks.decrementAndGet();
            submittedTasks.decrementAndGet();
            rejectedTasks.incrementAndGet();
            if (requestTask) {
                submittedRequestTasks.decrementAndGet();
                rejectedRequestTasks.incrementAndGet();
            } else {
                submittedResponseTasks.decrementAndGet();
                rejectedResponseTasks.incrementAndGet();
            }
            throw e;
        }
    }

    @Override
    public synchronized HealthReport checkHealth() {

        Map<String, Object> metrics = new LinkedHashMap<>();

        metrics.put("isQuiescing", isQuiescing);
        metrics.put("executorInitialized", messageWorker != null);

        metrics.put("submittedTasks", submittedTasks.get());
        metrics.put("completedTasks", completedTasks.get());
        metrics.put("failedTasks", failedTasks.get());
        metrics.put("rejectedTasks", rejectedTasks.get());
        metrics.put("rejectedDuringQuiesce", rejectedDuringQuiesce.get());

        metrics.put("activeTasks", activeTasks.get());
        metrics.put("outstandingTasks", outstandingTasks.get());

        metrics.put("submittedRequestTasks", submittedRequestTasks.get());
        metrics.put("completedRequestTasks", completedRequestTasks.get());
        metrics.put("failedRequestTasks", failedRequestTasks.get());
        metrics.put("rejectedRequestTasks", rejectedRequestTasks.get());

        metrics.put("submittedResponseTasks", submittedResponseTasks.get());
        metrics.put("completedResponseTasks", completedResponseTasks.get());
        metrics.put("failedResponseTasks", failedResponseTasks.get());
        metrics.put("rejectedResponseTasks", rejectedResponseTasks.get());

        metrics.put("retryResponseCount", retryRegistry != null);

        if (messageWorker != null) {
            metrics.put("executorShutdown", messageWorker.isShutdown());
            metrics.put("executorTerminated", messageWorker.isTerminated());
        }

        HealthStatus status;
        String message;

        if (messageWorker == null) {
            status = HealthStatus.UNREACHABLE;
            message = "Message service has not been initialized.";
        } else if (messageWorker.isTerminated() || messageWorker.isShutdown()) {
            status = HealthStatus.UNHEALTHY;
            message = "Message worker executor is no longer operational.";
        } else {

            boolean newTaskFailures =
                    failedTasks.get() > lastReportedFailedTasks;

            boolean newTaskRejections =
                    rejectedTasks.get() > lastReportedRejectedTasks;

            lastReportedFailedTasks = failedTasks.get();
            lastReportedRejectedTasks = rejectedTasks.get();

            if (newTaskFailures || newTaskRejections) {
                status = HealthStatus.DEGRADED;
                message = "Message service is operational but has experienced task failures or rejections.";
            } else {
                status = HealthStatus.HEALTHY;
                message = "Message service is operating normally.";
            }
        }

        return new HealthReport(
                "MessageService",
                status,
                message,
                Instant.now(),
                metrics
        );
    }
}