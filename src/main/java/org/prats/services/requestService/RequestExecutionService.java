
package org.prats.services.requestService;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.HealthStatus;
import org.prats.definitions.enums.ProcessType;

import org.prats.definitions.interfaces.HealthCheckable;
import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.definitions.interfaces.RequestHandler;
import org.prats.definitions.interfaces.Service;
import org.prats.definitions.interfaces.Submittable;

import org.prats.definitions.records.HealthReport;

import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;
import org.prats.threading.ThreadPool;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Bounded execution and lifecycle only.
 * Request meaning belongs to the injected handler.
 */
public class RequestExecutionService
        implements Service, Submittable, HealthCheckable {

    private static final EngineLogger log =
            EngineLogger.getLogger(RequestExecutionService.class);

    private final RequestHandler handler;
    private final MessageDispatcher dispatcher;

    private final Semaphore admission = new Semaphore(64);

    private final AtomicInteger active = new AtomicInteger();
    private final AtomicLong completed = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();

    private final Map<CompletableFuture<Message>, Message> pending =
            new ConcurrentHashMap<>();

    private volatile ExecutorService workers;
    private volatile boolean quiescing = true;


    // =========================================================
    // 1. CONSTRUCTOR
    // =========================================================

    public RequestExecutionService(
            RequestHandler handler,
            MessageDispatcher dispatcher
    ) {

        this.handler = Objects.requireNonNull(handler);
        this.dispatcher = Objects.requireNonNull(dispatcher);
    }


    // =========================================================
    // 2. SERVICE STARTUP
    // =========================================================

    @Override
    public synchronized void start() {

        if (workers != null && !workers.isTerminated()) {

            throw new IllegalStateException(
                    "Workers are already running"
            );
        }

        workers = new ThreadPool()
                .initializeAndGetThreadPool(
                        0,
                        0,
                        0,
                        false
                );

        quiescing = false;

        log.info(
                "[" + getName() + "][start] Service started"
        );
    }


    // =========================================================
    // 3. REQUEST SUBMISSION
    // =========================================================

    @Override
    public void submit(Message request) {

        ExecutorService executor = workers;

        if (quiescing
                || executor == null
                || !admission.tryAcquire()) {

            rejected.incrementAndGet();

            dispatcher.dispatchResponse(
                    failure(
                            request,
                            "Service is busy or unavailable. Try again later."
                    )
            );

            return;
        }

        active.incrementAndGet();

        CompletableFuture<Message> task =
                new CompletableFuture<>();

        pending.put(task, request);


        // -----------------------------------------------------
        // 3.1. Register task completion handler
        // -----------------------------------------------------

        task.whenComplete((response, error) -> {

            try {

                dispatcher.dispatchResponse(
                        error == null && response != null
                                ? response
                                : failure(
                                request,
                                "The service could not complete this request."
                        )
                );

            } finally {

                pending.remove(task);

                completed.incrementAndGet();

                active.decrementAndGet();

                admission.release();
            }
        });


        // -----------------------------------------------------
        // 3.2. Submit request to worker executor
        // -----------------------------------------------------

        try {

            executor.execute(() -> {

                if (task.isDone()) {
                    return;
                }

                try {

                    CompletableFuture<Message> execution =
                            handler.handleRequestAsync(request)
                                    .toCompletableFuture();


                    // -----------------------------------------
                    // Propagate cancellation to execution
                    // -----------------------------------------

                    task.whenComplete((ignored, error) -> {

                        if (task.isCancelled()) {
                            execution.cancel(true);
                        }
                    });


                    // -----------------------------------------
                    // Complete task with handler result
                    // -----------------------------------------

                    execution.whenComplete((response, error) -> {

                        if (error == null) {

                            task.complete(response);

                        } else {

                            task.completeExceptionally(error);
                        }
                    });

                } catch (RuntimeException error) {

                    task.completeExceptionally(error);
                }
            });


            // Shutdown may have raced admission before
            // the task was published.

            if (executor.isShutdown()) {
                task.cancel(true);
            }

        } catch (RejectedExecutionException error) {

            rejected.incrementAndGet();

            task.completeExceptionally(error);
        }
    }


    // =========================================================
    // 4. FAILURE RESPONSE CREATION
    // =========================================================

    private Message failure(
            Message request,
            String body
    ) {

        MessageParser parser =
                new MessageParser(ProcessType.PLATFORM);

        Message response = parser.createResponse(
                request.getCorrelationId(),
                Map.of()
        );

        response.setOperation(
                request.getOperation()
        );

        response.setPayload(
                parser.convertToJsonNode(
                        Map.of(
                                "ok", false,
                                "body", body
                        )
                )
        );

        response.setStatus("FAILED");

        response.setResponseComplete(true);

        return response;
    }


    // =========================================================
    // 5. SERVICE QUIESCING
    // =========================================================

    @Override
    public void quiesce() {

        quiescing = true;
    }


    // =========================================================
    // 6. SERVICE DRAINING
    // =========================================================

    @Override
    public void drain(long timeout) {

        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeout);

        while (active.get() > 0
                && System.nanoTime() < deadline) {

            LockSupport.parkNanos(
                    TimeUnit.MILLISECONDS.toNanos(10)
            );
        }
    }


    // =========================================================
    // 7. SERVICE SHUTDOWN
    // =========================================================

    @Override
    public synchronized void shutdown() {

        quiesce();

        if (workers != null) {
            workers.shutdownNow();
        }

        pending.keySet().forEach(
                future -> future.cancel(true)
        );
    }


    // =========================================================
    // 8. SERVICE CLEANUP
    // =========================================================

    @Override
    public void cleanup() {
    }


    // =========================================================
    // 9. SERVICE HEALTH CHECK
    // =========================================================

    @Override
    public HealthReport checkHealth() {

        ExecutorService executor = workers;

        boolean running = !quiescing
                && executor != null
                && !executor.isShutdown();

        return new HealthReport(
                getName(),

                running
                        ? HealthStatus.HEALTHY
                        : HealthStatus.UNREACHABLE,

                running
                        ? "Service is running."
                        : "Service is unavailable.",

                Instant.now(),

                Map.of(
                        "activeTasks", active.get(),
                        "completedTasks", completed.get(),
                        "rejectedTasks", rejected.get(),
                        "availablePermits", admission.availablePermits()
                )
        );
    }
}
