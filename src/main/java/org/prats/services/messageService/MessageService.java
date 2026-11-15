
package org.prats.services.messageService;

import org.prats.definitions.classes.Message;
import org.prats.definitions.classes.PendingDelivery;
import org.prats.definitions.enums.HealthStatus;
import org.prats.definitions.interfaces.HealthCheckable;
import org.prats.definitions.records.HealthReport;
import org.prats.loggers.EngineLogger;
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

    private static final EngineLogger log =
            EngineLogger.getLogger(MessageService.class);

    private final Object admissionLock;
    private final CorrelationRegistry correlationRegistry;
    private final RequestProcessor requestProcessor;
    private final ResponseProcessor responseProcessor;
    private final RetryRegistry retryRegistry;

    private ExecutorService messageWorker;
    private ExecutorService responseWorker;
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

        log.debug(() ->
                "[MessageService][constructor] Initializing MessageService"
                        + " | coreThread=" + coreThread
                        + " | maxThread=" + maxThread
                        + " | idleTimeout=" + idleTimeout
                        + " | retryInterval=" + retryInterval
                        + " | cleanupInterval=" + cleanupInterval
                        + " | correlationTimeout=" + correlationTimeout
        );

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

        log.info("[MessageService][constructor] MessageService initialized");
    }

    @Override
    public void start() {

        log.info("[MessageService][start] MessageService startup requested");

        log.debug(() ->
                "[MessageService][start] Initializing message worker executor"
                        + " | coreThread=" + coreThread
                        + " | maxThread=" + maxThread
                        + " | idleTimeout=" + idleTimeout
        );

        messageWorker = new ThreadPool().initializeAndGetThreadPool(
                coreThread,
                maxThread,
                idleTimeout,
                false
        );

        log.info("[MessageService][start] Message worker executor initialized");
        responseWorker = new ThreadPool().initializeAndGetThreadPool(coreThread, maxThread, idleTimeout, false);
        log.info("[MessageService][start] Reserved response/maintenance admission pool initialized");

        isQuiescing = false;

        log.debug("[MessageService][start] Message admission enabled");

        startBackgroundJobs();

        log.info("[MessageService][start] MessageService startup completed");
    }

    private void startBackgroundJobs() {

        log.info("[MessageService][startBackgroundJobs] Initializing background scheduler");

        backgroundScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "message-service-scheduler");
            thread.setDaemon(true);
            return thread;
        });

        log.debug("[MessageService][startBackgroundJobs] Background scheduler initialized");

        log.debug(() ->
                "[MessageService][startBackgroundJobs] Scheduling retry job"
                        + " | intervalSeconds=" + retryInterval
        );

        backgroundScheduler.scheduleWithFixedDelay(
                this::triggerRetryJob,
                retryInterval,
                retryInterval,
                TimeUnit.SECONDS
        );

        log.info("[MessageService][startBackgroundJobs] Retry job scheduled successfully");

        log.debug(() ->
                "[MessageService][startBackgroundJobs] Scheduling correlation cleanup job"
                        + " | intervalSeconds=" + cleanupInterval
        );

        backgroundScheduler.scheduleWithFixedDelay(
                this::triggerCorrelationCleanupJob,
                cleanupInterval,
                cleanupInterval,
                TimeUnit.SECONDS
        );

        log.info("[MessageService][startBackgroundJobs] Correlation cleanup job scheduled successfully");
    }

    public void releaseConnection(org.prats.definitions.classes.ClientConnection connection) {
        correlationRegistry.removeForConnection(connection);
        retryRegistry.removeForConnection(connection);
    }

    private void triggerRetryJob() {

        log.debug("[MessageService][triggerRetryJob] Retry job trigger invoked");

        if (!retryJobQueued.compareAndSet(false, true)) {

            log.debug("[MessageService][triggerRetryJob] Retry job already queued; skipping duplicate trigger");

            return;
        }

        log.debug("[MessageService][triggerRetryJob] Retry job admitted");

        try {

            submitToThreadPool(() -> {

                log.debug("[MessageService][triggerRetryJob] Retry worker task started");

                try {

                    retryFailedResponses();

                } finally {

                    retryJobQueued.set(false);

                    log.debug("[MessageService][triggerRetryJob] Retry job queue flag reset");
                }

            }, false);

            log.debug("[MessageService][triggerRetryJob] Retry job submitted successfully");

        } catch (RejectedExecutionException exception) {

            retryJobQueued.set(false);

            log.warn(() ->
                    "[MessageService][triggerRetryJob] Retry job rejected by message executor"
                            + " | exception=" + exception
            );
        }
    }


    private void triggerCorrelationCleanupJob() {

        log.debug("[MessageService][triggerCorrelationCleanupJob] Correlation cleanup trigger invoked");

        if (!cleanupJobQueued.compareAndSet(false, true)) {

            log.debug("[MessageService][triggerCorrelationCleanupJob] Cleanup job already queued; skipping duplicate trigger");

            return;
        }

        log.debug("[MessageService][triggerCorrelationCleanupJob] Cleanup job admitted");

        try {

            submitToThreadPool(() -> {

                log.debug("[MessageService][triggerCorrelationCleanupJob] Cleanup worker task started");

                try {

                    try {

                        cleanExpiredCorrelations();

                    } finally {

                        cleanExpiredPendingDeliveries();
                    }

                } finally {

                    cleanupJobQueued.set(false);

                    log.debug("[MessageService][triggerCorrelationCleanupJob] Cleanup job queue flag reset");
                }

            }, false);

            log.debug("[MessageService][triggerCorrelationCleanupJob] Cleanup job submitted successfully");

        } catch (RejectedExecutionException exception) {

            cleanupJobQueued.set(false);

            log.warn(() ->
                    "[MessageService][triggerCorrelationCleanupJob] Cleanup job rejected by message executor"
                            + " | exception=" + exception
            );
        }
    }



    private void retryFailedResponses() {

        log.debug("[MessageService][retryFailedResponses] Retrieving pending response failures");

        List<PendingDelivery> pendingDeliveries =
                retryRegistry.getAllFailures();

        log.debug(() ->
                "[MessageService][retryFailedResponses] Pending response failures retrieved"
                        + " | pendingDeliveries=" + pendingDeliveries.size()
        );

        for (PendingDelivery pendingDelivery : pendingDeliveries) {

            log.debug(() ->
                    "[MessageService][retryFailedResponses] Processing pending response delivery"
                            + " | correlationId=" + pendingDelivery.getCorrelationId()
                            + " | retryCount=" + pendingDelivery.getRetryCount()
            );

            responseProcessor.retry(pendingDelivery);

            log.debug(() ->
                    "[MessageService][retryFailedResponses] Retry processing completed"
                            + " | correlationId=" + pendingDelivery.getCorrelationId()
            );
        }
    }


    private void cleanExpiredCorrelations() {

        log.debug(() ->
                "[MessageService][cleanExpiredCorrelations] Cleaning expired correlations"
                        + " | correlationTimeout=" + correlationTimeout
        );

        List<CorrelationRegistry.ExpiredCorrelation> expired =
                correlationRegistry.removeExpired(correlationTimeout);

        log.debug(() ->
                "[MessageService][cleanExpiredCorrelations] Expired correlations retrieved"
                        + " | expiredCount=" + expired.size()
        );

        for (CorrelationRegistry.ExpiredCorrelation entry : expired) {

            log.warn(() ->
                    "[MessageService][cleanExpiredCorrelations] Processing expired correlation"
                            + " | correlationId=" + entry.correlationId()
            );

            responseProcessor.processExpiry(
                    entry.correlationId(),
                    entry.replyChannel()
            );

            log.debug(() ->
                    "[MessageService][cleanExpiredCorrelations] Expired correlation processing completed"
                            + " | correlationId=" + entry.correlationId()
            );
        }
    }


    private void cleanExpiredPendingDeliveries() {

        log.debug("[MessageService][cleanExpiredPendingDeliveries] Checking expired pending deliveries");

        List<PendingDelivery> expiredDeliveries =
                retryRegistry.removeExpired();

        log.debug(() ->
                "[MessageService][cleanExpiredPendingDeliveries] Expired pending deliveries retrieved"
                        + " | expiredCount=" + expiredDeliveries.size()
        );

        for (PendingDelivery pendingDelivery : expiredDeliveries) {

            log.warn(() ->
                    "[MessageService][cleanExpiredPendingDeliveries] Pending response delivery expired"
                            + " | correlationId=" + pendingDelivery.getCorrelationId()
                            + " | retryCount=" + pendingDelivery.getRetryCount()
            );
        }
    }


    @Override
    public void shutdown() {

        log.info("[MessageService][shutdown] MessageService shutdown requested");

        if (backgroundScheduler != null) {

            log.debug("[MessageService][shutdown] Shutting down background scheduler");

            backgroundScheduler.shutdownNow();

            log.info("[MessageService][shutdown] Background scheduler shutdown requested");
        }

        log.debug("[MessageService][shutdown] Shutting down message worker executor");

        if (messageWorker != null) messageWorker.shutdownNow();
        if (responseWorker != null) responseWorker.shutdownNow();

        log.info("[MessageService][shutdown] Message worker shutdown requested");
    }

    @Override
    public void quiesce() {

        log.info("[MessageService][quiesce] MessageService quiescence requested");

        synchronized (admissionLock) {

            isQuiescing = true;
        }

        log.debug("[MessageService][quiesce] Message admission disabled");

        log.info("[MessageService][quiesce] MessageService is now quiescing");
    }

    @Override
    public void drain(long timeout) {

        log.info(() ->
                "[MessageService][drain] MessageService drain requested"
                        + " | timeoutMillis=" + timeout
                        + " | outstandingTasks=" + outstandingTasks.get()
                        + " | correlationCount=" + correlationRegistry.getSize()
        );

        // check for work completion for a certain period
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);

        while (hasOutstandingWork()) {

            if (System.nanoTime() >= deadline) {

                log.warn(() ->
                        "[MessageService][drain] Drain deadline reached with outstanding work"
                                + " | outstandingTasks=" + outstandingTasks.get()
                                + " | correlationCount=" + correlationRegistry.getSize()
                                + " | timeoutMillis=" + timeout
                );

                break;
            }

            LockSupport.parkNanos(
                    TimeUnit.MILLISECONDS.toNanos(100)
            );
        }

        log.debug(() ->
                "[MessageService][drain] Drain operation completed"
                        + " | outstandingTasks=" + outstandingTasks.get()
                        + " | correlationCount=" + correlationRegistry.getSize()
        );
    }

    @Override
    public void cleanup() {

        log.debug("[MessageService][cleanup] Cleanup invoked");
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

        log.debug("[MessageService][dispatchRequest] Request dispatch received");

        try {
            synchronized (admissionLock) {
                if (isQuiescing) {
                    rejectedDuringQuiesce.incrementAndGet();
                    throw new RejectedExecutionException("MessageService is quiescing");
                }
                submitToThreadPool(() -> requestProcessor.process(message, replyChannel), true);
            }
        } catch (RejectedExecutionException exception) {
            log.warn("[MessageService][dispatchRequest] Request rejected by admission control");
            var operation = message.getOperation() == org.prats.definitions.enums.Operation.UNKNOWN
                    ? org.prats.definitions.enums.Operation.COMMAND : message.getOperation();
            Message rejected = new Message(org.prats.definitions.enums.MessageType.RESPONSE,
                    org.prats.definitions.enums.ProcessType.PLATFORM, operation, null,
                    null, null, message.getCorrelationId());
            new org.prats.parsers.MessageParser(org.prats.definitions.enums.ProcessType.PLATFORM)
                    .attachError(rejected, "OVERLOADED", "Message service is busy or quiescing. Retry later.");
            replyChannel.sendReply(rejected);
        }

        log.debug("[MessageService][dispatchRequest] Request task submitted");
    }

    // handled by response processor
    @Override
    public void dispatchResponse(Message message) {

        log.debug("[MessageService][dispatchResponse] Response dispatch received");

        try {
            submitToThreadPool(() -> responseProcessor.process(message), false);
        } catch (RejectedExecutionException exception) {
            // Responses must free correlations even when request admission is saturated.
            log.warn("[MessageService][dispatchResponse] Response pool saturated; completing response on caller");
            responseProcessor.process(message);
        }

        log.debug("[MessageService][dispatchResponse] Response task submitted");
    }

    private void submitToThreadPool(Runnable runnable, boolean requestTask) {

        log.debug(() ->
                "[MessageService][submitToThreadPool] Preparing task submission"
                        + " | requestTask=" + requestTask
                        + " | activeTasks=" + activeTasks.get()
                        + " | outstandingTasks=" + outstandingTasks.get()
        );

        outstandingTasks.incrementAndGet();
        submittedTasks.incrementAndGet();

        if (requestTask) {

            submittedRequestTasks.incrementAndGet();

        } else {

            submittedResponseTasks.incrementAndGet();
        }

        log.debug(() ->
                "[MessageService][submitToThreadPool] Task counters incremented"
                        + " | requestTask=" + requestTask
                        + " | submittedTasks=" + submittedTasks.get()
                        + " | submittedRequestTasks=" + submittedRequestTasks.get()
                        + " | submittedResponseTasks=" + submittedResponseTasks.get()
                        + " | outstandingTasks=" + outstandingTasks.get()
        );

        try {

            (requestTask ? messageWorker : responseWorker).submit(() -> {

                int currentActiveTasks = activeTasks.incrementAndGet();

                log.debug(() ->
                        "[MessageService][submitToThreadPool] Worker task started"
                                + " | requestTask=" + requestTask
                                + " | activeTasks=" + currentActiveTasks
                                + " | outstandingTasks=" + outstandingTasks.get()
                );

                try {

                    runnable.run();

                    log.debug("[MessageService][submitToThreadPool] Worker task execution completed");

                } catch (Exception e) {

                    long currentFailedTasks = failedTasks.incrementAndGet();

                    if (requestTask) {

                        long currentFailedRequestTasks = failedRequestTasks.incrementAndGet();

                        log.error(
                                "[MessageService][submitToThreadPool] Request task failed"
                                        + " | failedTasks=" + currentFailedTasks
                                        + " | failedRequestTasks=" + currentFailedRequestTasks,
                                e
                        );

                    } else {

                        long currentFailedResponseTasks = failedResponseTasks.incrementAndGet();

                        log.error(
                                "[MessageService][submitToThreadPool] Response task failed"
                                        + " | failedTasks=" + currentFailedTasks
                                        + " | failedResponseTasks=" + currentFailedResponseTasks,
                                e
                        );
                    }

                    throw e;

                } finally {

                    long currentCompletedTasks = completedTasks.incrementAndGet();

                    int remainingActiveTasks = activeTasks.decrementAndGet();

                    int remainingOutstandingTasks = outstandingTasks.decrementAndGet();

                    if (requestTask) {

                        long currentCompletedRequestTasks = completedRequestTasks.incrementAndGet();

                        log.debug(() ->
                                "[MessageService][submitToThreadPool] Request task counters updated"
                                        + " | completedTasks=" + currentCompletedTasks
                                        + " | completedRequestTasks=" + currentCompletedRequestTasks
                                        + " | activeTasks=" + remainingActiveTasks
                                        + " | outstandingTasks=" + remainingOutstandingTasks
                        );

                    } else {

                        long currentCompletedResponseTasks = completedResponseTasks.incrementAndGet();

                        log.debug(() ->
                                "[MessageService][submitToThreadPool] Response task counters updated"
                                        + " | completedTasks=" + currentCompletedTasks
                                        + " | completedResponseTasks=" + currentCompletedResponseTasks
                                        + " | activeTasks=" + remainingActiveTasks
                                        + " | outstandingTasks=" + remainingOutstandingTasks
                        );
                    }
                }
            });

            log.debug("[MessageService][submitToThreadPool] Task accepted by message worker executor");

        } catch (RejectedExecutionException e) {

            outstandingTasks.decrementAndGet();
            submittedTasks.decrementAndGet();

            long currentRejectedTasks = rejectedTasks.incrementAndGet();

            if (requestTask) {

                long currentRejectedRequestTasks = rejectedRequestTasks.incrementAndGet();

                submittedRequestTasks.decrementAndGet();

                log.warn(() ->
                        "[MessageService][submitToThreadPool] Request task rejected by executor"
                                + " | rejectedTasks=" + currentRejectedTasks
                                + " | rejectedRequestTasks=" + currentRejectedRequestTasks
                                + " | outstandingTasks=" + outstandingTasks.get()
                );

            } else {

                long currentRejectedResponseTasks = rejectedResponseTasks.incrementAndGet();

                submittedResponseTasks.decrementAndGet();

                log.warn(() ->
                        "[MessageService][submitToThreadPool] Response task rejected by executor"
                                + " | rejectedTasks=" + currentRejectedTasks
                                + " | rejectedResponseTasks=" + currentRejectedResponseTasks
                                + " | outstandingTasks=" + outstandingTasks.get()
                );
            }

            throw e;
        }
    }


    @Override
    public synchronized HealthReport checkHealth() {

        log.debug("[MessageService][checkHealth] MessageService health check started");

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.putAll(org.prats.threading.ThreadPool.admissionMetrics(messageWorker));
        metrics.put("responseAdmission", org.prats.threading.ThreadPool.admissionMetrics(responseWorker));
        metrics.put("correlationEntries", correlationRegistry.getSize());
        metrics.put("correlationLimit", correlationRegistry.capacity());
        metrics.put("correlationRejections", correlationRegistry.rejectedCount());
        metrics.put("retryEntries", retryRegistry.size());
        metrics.put("retryRetainedBytes", retryRegistry.pendingBytes());

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

            log.warn("[MessageService][checkHealth] Message service has not been initialized");

        } else if (messageWorker.isTerminated() || messageWorker.isShutdown()) {

            status = HealthStatus.UNHEALTHY;

            message = "Message worker executor is no longer operational.";

            log.error("[MessageService][checkHealth] Message worker executor is no longer operational");

        } else {

            boolean newTaskFailures =
                    failedTasks.get() > lastReportedFailedTasks;

            boolean newTaskRejections =
                    rejectedTasks.get() > lastReportedRejectedTasks;

            log.debug(() ->
                    "[MessageService][checkHealth] Evaluating task failure and rejection counters"
                            + " | newTaskFailures=" + newTaskFailures
                            + " | newTaskRejections=" + newTaskRejections
                            + " | failedTasks=" + failedTasks.get()
                            + " | rejectedTasks=" + rejectedTasks.get()
            );

            lastReportedFailedTasks = failedTasks.get();
            lastReportedRejectedTasks = rejectedTasks.get();

            if (newTaskFailures || newTaskRejections) {

                status = HealthStatus.DEGRADED;

                message = "Message service is operational but has experienced task failures or rejections.";

                log.warn("[MessageService][checkHealth] Message service is operational but degraded due to task failures or rejections");

            } else {

                status = HealthStatus.HEALTHY;

                message = "Message service is operating normally.";

                log.debug("[MessageService][checkHealth] Message service is healthy");
            }
        }

        final HealthStatus finalStatus = status;

        log.info(() ->
                "[MessageService][checkHealth] MessageService health evaluation completed"
                        + " | status=" + finalStatus
                        + " | activeTasks=" + activeTasks.get()
                        + " | outstandingTasks=" + outstandingTasks.get()
                        + " | submittedTasks=" + submittedTasks.get()
                        + " | completedTasks=" + completedTasks.get()
                        + " | failedTasks=" + failedTasks.get()
                        + " | rejectedTasks=" + rejectedTasks.get()
        );

        return new HealthReport(
                "MessageService",
                status,
                message,
                Instant.now(),
                metrics
        );
    }
}
