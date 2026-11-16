
package org.prats.services.authenticationService;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.HealthStatus;

import org.prats.definitions.interfaces.Authenticator;
import org.prats.definitions.interfaces.HealthCheckable;
import org.prats.definitions.interfaces.Service;

import org.prats.definitions.records.HealthReport;

import org.prats.handlers.AuthenticationHandler;
import org.prats.loggers.EngineLogger;
import org.prats.managers.InMemorySessionManager;
import org.prats.threading.ThreadPool;

import java.time.Instant;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import java.util.concurrent.locks.LockSupport;

/**
 * Platform authentication service.
 *
 * Implements the standard message-based authentication
 * contract used by Mediation Engine services.
 *
 * Responsibilities:
 *
 * 1. Accept authentication Messages.
 * 2. Apply non-blocking request admission.
 * 3. Submit accepted requests to a worker executor.
 * 4. Delegate business processing to AuthenticationHandler.
 * 5. Track outstanding authentication operations.
 * 6. Support quiescence, draining and shutdown.
 * 7. Perform scheduled housekeeping.
 * 8. Expose operational metrics and health reports.
 *
 * AuthenticationHandler performs business operations
 * and dispatches response Messages through MessageService.
 *
 * MessageService owns request/response correlation and
 * ReplyChannel delivery.
 *
 * AuthenticationService does not return CompletionStage
 * results or maintain a second response-correlation system.
 *
 * IMPORTANT:
 *
 * A handler may return from processRequest() after
 * dispatching an asynchronous database request.
 *
 * Consequently, the end of a worker Runnable does not
 * necessarily represent completion of authentication.
 *
 * A terminal response notification must be delivered
 * through onResponseDispatched(), or a terminal failure
 * notification through onRequestFailed().
 *
 * Session identity is independent of TCP connections.
 */
public final class AuthenticationService
        implements Service, Authenticator, HealthCheckable {

    private static final EngineLogger log =
            EngineLogger.getLogger(AuthenticationService.class);

    private static final String SERVICE_NAME =
            "AuthenticationService";

    // =========================================================
    // 1. DEFAULT CONFIGURATION
    // =========================================================

    /**
     * Default maximum number of authentication operations
     * admitted simultaneously.
     *
     * Includes:
     *
     * - Queued worker tasks.
     * - Executing worker tasks.
     * - Operations awaiting asynchronous database responses.
     */
    private static final int DEFAULT_MAX_IN_FLIGHT = 8;

    /**
     * Frequency of housekeeping operations.
     */
    private static final long CLEANUP_INTERVAL_SECONDS = 30;

    /**
     * Threshold for identifying unusually long-running
     * authentication operations.
     *
     * This is a diagnostic threshold, not a timeout.
     */
    private static final long STALE_REQUEST_SECONDS = 30;

    /**
     * Default time allowed for graceful draining.
     */
    private static final long SHUTDOWN_TIMEOUT_MILLIS = 5_000;

    // =========================================================
    // 2. THREAD CONFIGURATION
    // =========================================================

    private final int coreThreads;

    private final int maxThreads;

    private final long idleTimeout;

    private final boolean createPlatformThread;

    /**
     * Maximum number of complete authentication
     * operations permitted in flight.
     */
    private final int maxInFlightRequests;

    // =========================================================
    // 3. SERVICE DEPENDENCIES
    // =========================================================

    /**
     * Executes authentication business workflows.
     *
     * The handler receives and produces standard
     * Mediation Engine Messages.
     */
    private final AuthenticationHandler authenticationHandler;

    /**
     * Optional session-management dependency.
     *
     * When configured, the housekeeping scheduler
     * periodically removes expired sessions.
     *
     * AuthenticationHandler and the authorization layer
     * must use the same shared session manager instance.
     */
    private final InMemorySessionManager sessionManager;

    // =========================================================
    // 4. ADMISSION AND LIFECYCLE
    // =========================================================

    /**
     * Coordinates admission with lifecycle transitions.
     *
     * New work cannot be accepted concurrently with
     * a transition into quiescence or shutdown.
     */
    private final Object admissionLock =
            new Object();

    /**
     * Controls complete authentication-operation admission.
     *
     * A permit is acquired when a request enters the service.
     *
     * It is released after a terminal response is dispatched,
     * or after the operation fails terminally.
     *
     * tryAcquire() never waits for capacity.
     */
    private final Semaphore requestPermits;

    private volatile ExecutorService executorService;

    private volatile ScheduledExecutorService scheduledExecutorService;

    /**
     * New requests are rejected while quiescing.
     *
     * The service initially starts in a non-admitting state.
     */
    private volatile boolean isQuiescing = true;

    private volatile Instant startedAt;

    // =========================================================
    // 5. EXECUTION METRICS
    // =========================================================

    /**
     * Successfully submitted worker tasks.
     */
    private final AtomicLong submittedTasks =
            new AtomicLong();

    /**
     * Authentication operations whose terminal responses
     * were accepted for dispatch by MessageService.
     *
     * This does not guarantee successful TCP delivery.
     */
    private final AtomicLong completedRequests =
            new AtomicLong();

    /**
     * Terminal responses containing structured errors.
     *
     * Includes authentication denials and other
     * application-level error responses.
     */
    private final AtomicLong errorResponses =
            new AtomicLong();

    /**
     * Requests that failed without successfully
     * dispatching a terminal response.
     */
    private final AtomicLong failedRequests =
            new AtomicLong();

    /**
     * Requests rejected before worker admission.
     */
    private final AtomicLong rejectedRequests =
            new AtomicLong();

    /**
     * Admitted worker tasks cancelled before execution.
     */
    private final AtomicLong cancelledTasks =
            new AtomicLong();

    /**
     * Requests currently executing inside worker tasks.
     *
     * This metric is distinct from outstandingRequests.
     */
    private final AtomicInteger activeWorkerTasks =
            new AtomicInteger();

    /**
     * Admitted operations not yet terminally completed.
     *
     * The counter includes asynchronous work whose
     * initial worker task has already returned.
     */
    private final AtomicInteger outstandingRequests =
            new AtomicInteger();

    /**
     * Number of outstanding operations exceeding
     * the diagnostic stale-request threshold.
     */
    private final AtomicInteger staleRequests =
            new AtomicInteger();

    private final AtomicLong staleWarningCount =
            new AtomicLong();

    private final AtomicLong cleanupFailures =
            new AtomicLong();

    // =========================================================
    // 6. IN-FLIGHT REQUEST TRACKING
    // =========================================================

    /**
     * Tracks service admission lifecycle.
     *
     * Key:
     *     Original request correlation UUID.
     *
     * Value:
     *     Authentication task lifecycle.
     *
     * This map is NOT a response-correlation registry.
     *
     * MessageService remains responsible for matching
     * response Messages with ReplyChannels.
     *
     * This registry exists only to:
     *
     * - Release service admission.
     * - Track outstanding operations.
     * - Support draining.
     * - Detect stalled operations.
     *
     * The inbound MessageService must ensure that active
     * request correlation identifiers are unique.
     */
    private final Map<UUID, AuthenticationTask> pendingRequests =
            new ConcurrentHashMap<>();

    // =========================================================
    // 7. CONSTRUCTORS
    // =========================================================

    /**
     * Constructor using the default admission limit.
     *
     * Session cleanup is not enabled unless a session
     * manager is explicitly supplied.
     */
    public AuthenticationService(
            int coreThreads,
            int maxThreads,
            long idleTimeout,
            boolean createPlatformThread,
            AuthenticationHandler authenticationHandler
    ) {

        this(
                coreThreads,
                maxThreads,
                idleTimeout,
                createPlatformThread,
                Integer.getInteger(
                        "mediation.authentication.max-inflight",
                        DEFAULT_MAX_IN_FLIGHT
                ),
                authenticationHandler,
                null
        );
    }

    /**
     * Constructor using an explicitly configured
     * authentication admission limit.
     */
    public AuthenticationService(
            int coreThreads,
            int maxThreads,
            long idleTimeout,
            boolean createPlatformThread,
            int maxInFlightRequests,
            AuthenticationHandler authenticationHandler
    ) {

        this(
                coreThreads,
                maxThreads,
                idleTimeout,
                createPlatformThread,
                maxInFlightRequests,
                authenticationHandler,
                null
        );
    }

    /**
     * Constructor with complete service configuration
     * and an optional shared session manager.
     *
     * @param coreThreads Core platform-thread count.
     * @param maxThreads Maximum platform-thread count.
     * @param idleTimeout Worker idle timeout in milliseconds.
     * @param createPlatformThread Selects platform threads.
     * @param maxInFlightRequests Maximum admitted operations.
     * @param authenticationHandler Authentication business handler.
     * @param sessionManager Shared session manager, or null.
     */
    public AuthenticationService(
            int coreThreads,
            int maxThreads,
            long idleTimeout,
            boolean createPlatformThread,
            int maxInFlightRequests,
            AuthenticationHandler authenticationHandler,
            InMemorySessionManager sessionManager
    ) {

        // -----------------------------------------------------
        // VALIDATE ADMISSION LIMIT
        // -----------------------------------------------------

        if (maxInFlightRequests < 1) {

            throw new IllegalArgumentException(
                    "Maximum in-flight requests must be positive"
            );
        }

        // -----------------------------------------------------
        // VALIDATE PLATFORM-THREAD CONFIGURATION
        // -----------------------------------------------------

        if (createPlatformThread) {

            if (coreThreads < 0
                    || maxThreads < 1
                    || maxThreads < coreThreads
                    || idleTimeout < 0) {

                throw new IllegalArgumentException(
                        "Invalid platform-thread configuration"
                );
            }
        }

        // -----------------------------------------------------
        // INITIALIZE CONFIGURATION
        // -----------------------------------------------------

        this.coreThreads = coreThreads;
        this.maxThreads = maxThreads;
        this.idleTimeout = idleTimeout;
        this.createPlatformThread = createPlatformThread;
        this.maxInFlightRequests = maxInFlightRequests;

        // -----------------------------------------------------
        // INITIALIZE DEPENDENCIES
        // -----------------------------------------------------

        this.authenticationHandler =
                Objects.requireNonNull(
                        authenticationHandler,
                        "AuthenticationHandler cannot be null"
                );

        this.sessionManager = sessionManager;

        // -----------------------------------------------------
        // INITIALIZE REQUEST ADMISSION
        // -----------------------------------------------------

        this.requestPermits =
                new Semaphore(maxInFlightRequests);

        log.info(() ->
                "[AuthenticationService][constructor] "
                        + "Authentication service configured"
                        + " | coreThreads=" + coreThreads
                        + " | maxThreads=" + maxThreads
                        + " | idleTimeoutMillis=" + idleTimeout
                        + " | platformThreads=" + createPlatformThread
                        + " | maxInFlightRequests=" + maxInFlightRequests
                        + " | sessionCleanupEnabled="
                        + (sessionManager != null)
        );
    }

    // =========================================================
    // 8. AUTHENTICATION REQUEST ADMISSION
    // =========================================================

    /**
     * Accepts an authentication Message for processing.
     *
     * Processing sequence:
     *
     * 1. Validate request and correlation identifier.
     * 2. Check service lifecycle state.
     * 3. Acquire an in-flight admission permit.
     * 4. Register request lifecycle tracking.
     * 5. Submit the worker task.
     *
     * The method does not wait for authentication
     * or database processing to complete.
     *
     * RejectedExecutionException is propagated to
     * the forwarding/dispatch layer.
     *
     * The forwarding layer must translate rejection
     * into a standard structured error Message.
     */
    @Override
    public void processRequest(
            Message request
    ) {

        Objects.requireNonNull(
                request,
                "Authentication request cannot be null"
        );

        UUID correlationId =
                Objects.requireNonNull(
                        request.getCorrelationId(),
                        "Authentication correlation ID cannot be null"
                );

        synchronized (admissionLock) {

            ExecutorService workers =
                    executorService;

            // -------------------------------------------------
            // VALIDATE SERVICE STATE
            // -------------------------------------------------

            if (isQuiescing
                    || workers == null
                    || workers.isShutdown()) {

                recordRejection(
                        "SERVICE_UNAVAILABLE"
                );

                throw new RejectedExecutionException(
                        "Authentication service is not accepting requests"
                );
            }

            // -------------------------------------------------
            // ACQUIRE REQUEST ADMISSION
            // -------------------------------------------------

            if (!requestPermits.tryAcquire()) {

                recordRejection(
                        "ADMISSION_LIMIT_REACHED"
                );

                throw new RejectedExecutionException(
                        "Authentication request admission limit reached"
                );
            }

            // -------------------------------------------------
            // REGISTER REQUEST LIFECYCLE
            // -------------------------------------------------

            AuthenticationTask task =
                    new AuthenticationTask(
                            correlationId,
                            request
                    );

            /*
             * Reject duplicate active correlation IDs.
             *
             * An existing request must never have its
             * admission tracking silently overwritten.
             */
            AuthenticationTask existing =
                    pendingRequests.putIfAbsent(
                            correlationId,
                            task
                    );

            if (existing != null) {

                requestPermits.release();

                recordRejection(
                        "DUPLICATE_CORRELATION"
                );

                throw new RejectedExecutionException(
                        "Authentication correlation already active"
                );
            }

            outstandingRequests.incrementAndGet();

            // -------------------------------------------------
            // SUBMIT TO WORKER EXECUTOR
            // -------------------------------------------------

            try {

                submitToThreadPool(task);

                submittedTasks.incrementAndGet();

                log.debug(() ->
                        "[AuthenticationService][processRequest] "
                                + "Authentication request admitted"
                                + " | outstandingRequests="
                                + outstandingRequests.get()
                                + " | activeWorkers="
                                + activeWorkerTasks.get()
                );

            } catch (RuntimeException exception) {

                /*
                 * Submission failed before the task
                 * entered the delegate executor.
                 *
                 * Release the reserved admission state.
                 */
                task.cancelBeforeExecution();

                recordRejection(
                        "WORKER_SUBMISSION_FAILED"
                );

                log.error(
                        "[AuthenticationService][processRequest] "
                                + "Worker submission failed",
                        exception
                );

                throw exception;
            }
        }
    }

    // =========================================================
    // 9. INTERNAL AUTHENTICATION TASK
    // =========================================================

    /**
     * Worker task for an admitted authentication request.
     *
     * The Runnable performs initial handler dispatch.
     *
     * The asynchronous authentication operation may
     * continue after run() has returned.
     *
     * Admission is therefore not released in run()'s
     * finally block.
     */
    private final class AuthenticationTask
            implements Runnable {

        private final UUID correlationId;

        /**
         * The request is retained only while queued
         * or being dispatched to AuthenticationHandler.
         *
         * After worker execution, this reference is
         * cleared to avoid retaining sensitive payloads
         * solely for admission bookkeeping.
         */
        private volatile Message request;

        private final long createdAtNanos =
                System.nanoTime();

        /**
         * Prevents execution after queued cancellation.
         */
        private final AtomicBoolean started =
                new AtomicBoolean();

        /**
         * Prevents duplicate completion and permit release.
         */
        private final AtomicBoolean finalized =
                new AtomicBoolean();

        private AuthenticationTask(
                UUID correlationId,
                Message request
        ) {

            this.correlationId = correlationId;
            this.request = request;
        }

        // -----------------------------------------------------
        // WORKER EXECUTION
        // -----------------------------------------------------

        @Override
        public void run() {

            if (!started.compareAndSet(false, true)) {
                return;
            }

            if (finalized.get()) {
                return;
            }

            activeWorkerTasks.incrementAndGet();

            try {

                Message currentRequest = request;

                if (currentRequest == null) {

                    throw new IllegalStateException(
                            "Authentication request was released before execution"
                    );
                }

                log.debug(() ->
                        "[AuthenticationService][AuthenticationTask] "
                                + "Authentication worker started"
                                + " | activeWorkers="
                                + activeWorkerTasks.get()
                );

                // ---------------------------------------------
                // DELEGATE TO AUTHENTICATION HANDLER
                // ---------------------------------------------

                /*
                 * The handler owns the authentication
                 * workflow and its response Messages.
                 *
                 * Internal database calls may continue
                 * asynchronously through MessageService.
                 *
                 * Handler return is not treated as
                 * completion of authentication.
                 */
                authenticationHandler.processRequest(
                        currentRequest
                );

            } catch (RuntimeException exception) {

                /*
                 * An unhandled handler exception means
                 * the operation could not proceed.
                 *
                 * Terminal failure tracking is released.
                 *
                 * A structured error response must be
                 * produced by the handler or response
                 * infrastructure for the original request.
                 */
                onRequestFailed(
                        correlationId,
                        exception
                );

                log.error(
                        "[AuthenticationService][AuthenticationTask] "
                                + "Authentication handler execution failed",
                        exception
                );

            } catch (Error error) {

                onRequestFailed(
                        correlationId,
                        error
                );

                throw error;

            } finally {

                /*
                 * Worker activity ends here.
                 *
                 * Complete-operation admission remains
                 * occupied until a terminal notification.
                 */
                request = null;

                activeWorkerTasks.decrementAndGet();

                log.debug(() ->
                        "[AuthenticationService][AuthenticationTask] "
                                + "Initial handler execution completed"
                                + " | activeWorkers="
                                + activeWorkerTasks.get()
                                + " | outstandingRequests="
                                + outstandingRequests.get()
                );
            }
        }

        // -----------------------------------------------------
        // COMPLETE RESPONSE DISPATCH
        // -----------------------------------------------------

        /**
         * Marks an operation complete after its final
         * response has been submitted to MessageService.
         *
         * A response containing an error is still a
         * completed request/response workflow.
         */
        private void complete(
                Message response
        ) {

            if (!finalizeRequest()) {
                return;
            }

            completedRequests.incrementAndGet();

            if (response.hasError()) {
                errorResponses.incrementAndGet();
            }

            log.debug(() ->
                    "[AuthenticationService][AuthenticationTask] "
                            + "Authentication response dispatched"
                            + " | hasError=" + response.hasError()
                            + " | outstandingRequests="
                            + outstandingRequests.get()
            );
        }

        // -----------------------------------------------------
        // TERMINAL FAILURE
        // -----------------------------------------------------

        /**
         * Marks a request as terminally failed when
         * normal final-response dispatch cannot proceed.
         */
        private void fail(
                Throwable cause
        ) {

            if (!finalizeRequest()) {
                return;
            }

            failedRequests.incrementAndGet();

            log.warn(() ->
                    "[AuthenticationService][AuthenticationTask] "
                            + "Authentication request failed terminally"
                            + " | failureType="
                            + cause.getClass().getSimpleName()
                            + " | outstandingRequests="
                            + outstandingRequests.get()
            );
        }

        // -----------------------------------------------------
        // CANCEL BEFORE WORKER EXECUTION
        // -----------------------------------------------------

        /**
         * Cancels a queued task that never started.
         *
         * Used during failed submission or forced shutdown.
         */
        private void cancelBeforeExecution() {

            if (!started.compareAndSet(false, true)) {
                return;
            }

            if (!finalizeRequest()) {
                return;
            }

            cancelledTasks.incrementAndGet();

            log.debug(
                    "[AuthenticationService][AuthenticationTask] "
                            + "Queued authentication task cancelled"
            );
        }

        // -----------------------------------------------------
        // RELEASE REQUEST RESOURCES
        // -----------------------------------------------------

        /**
         * Performs terminal request cleanup exactly once.
         *
         * This method must not be called merely because
         * the initial worker Runnable has returned.
         */
        private boolean finalizeRequest() {

            if (!finalized.compareAndSet(false, true)) {
                return false;
            }

            pendingRequests.remove(
                    correlationId,
                    this
            );

            /*
             * Release the queued request reference.
             *
             * AuthenticationHandler owns references
             * required by any remaining workflow.
             */
            request = null;

            outstandingRequests.decrementAndGet();

            requestPermits.release();

            return true;
        }

        // -----------------------------------------------------
        // REQUEST AGE
        // -----------------------------------------------------

        private long ageSeconds() {

            long elapsed =
                    System.nanoTime() - createdAtNanos;

            return Math.max(
                    0,
                    TimeUnit.NANOSECONDS.toSeconds(elapsed)
            );
        }
    }

    // =========================================================
    // 10. TERMINAL RESPONSE NOTIFICATION
    // =========================================================

    /**
     * Called after the final authentication response
     * has been accepted by MessageService for dispatch.
     *
     * The response must preserve the original
     * authentication request correlation ID.
     *
     * This callback must not be invoked for internal
     * DatabaseService responses.
     *
     * Notification is idempotent.
     *
     * Successful response dispatch does not guarantee
     * successful TCP delivery to the requesting client.
     *
     * @param response Final authentication response.
     */
    public void onResponseDispatched(
            Message response
    ) {

        Objects.requireNonNull(
                response,
                "Authentication response cannot be null"
        );

        UUID correlationId =
                response.getCorrelationId();

        if (correlationId == null) {

            log.warn(
                    "[AuthenticationService][onResponseDispatched] "
                            + "Response completion ignored: missing correlation ID"
            );

            return;
        }

        AuthenticationTask task =
                pendingRequests.get(correlationId);

        if (task == null) {

            log.debug(
                    "[AuthenticationService][onResponseDispatched] "
                            + "No matching authentication operation found"
            );

            return;
        }

        task.complete(response);
    }

    // =========================================================
    // 11. TERMINAL FAILURE NOTIFICATION
    // =========================================================

    /**
     * Signals terminal failure of an authentication
     * workflow when no final response can be dispatched.
     *
     * This may be called by an asynchronous handler
     * continuation after database or message-processing
     * failure.
     *
     * It releases service admission bookkeeping.
     *
     * It does not independently construct an error Message.
     *
     * Structured error handling and correlation cleanup
     * must remain coordinated with MessageService.
     */
    public void onRequestFailed(
            UUID correlationId,
            Throwable cause
    ) {

        if (correlationId == null) {
            return;
        }

        AuthenticationTask task =
                pendingRequests.get(correlationId);

        if (task == null) {
            return;
        }

        Throwable failure =
                cause != null
                        ? cause
                        : new IllegalStateException(
                        "Authentication request failed"
                );

        task.fail(failure);
    }

    // =========================================================
    // 12. RECORD REJECTION
    // =========================================================

    /**
     * Updates admission-rejection metrics.
     *
     * Repeated warnings are rate-limited using
     * power-of-two logging intervals.
     */
    private void recordRejection(
            String reason
    ) {

        long count =
                rejectedRequests.incrementAndGet();

        if (isPowerOfTwo(count)) {

            log.warn(
                    "[AuthenticationService][recordRejection] "
                            + "Authentication request rejected"
                            + " | reason=" + reason
                            + " | maxInFlightRequests="
                            + maxInFlightRequests
                            + " | outstandingRequests="
                            + outstandingRequests.get()
                            + " | totalRejected=" + count
            );
        }
    }

    // =========================================================
    // 13. SERVICE STARTUP
    // =========================================================

    /**
     * Initializes the worker executor and housekeeping
     * scheduler.
     *
     * Startup is serialized against admission,
     * quiescence and shutdown transitions.
     */
    @Override
    public void start() {

        synchronized (admissionLock) {

            // -------------------------------------------------
            // VALIDATE EXISTING EXECUTOR STATE
            // -------------------------------------------------

            if (executorService != null
                    && !executorService.isTerminated()) {

                throw new IllegalStateException(
                        "Authentication worker executor is already active"
                );
            }

            if (scheduledExecutorService != null
                    && !scheduledExecutorService.isTerminated()) {

                throw new IllegalStateException(
                        "Authentication scheduler is already active"
                );
            }

            if (hasOutstandingWork()) {

                throw new IllegalStateException(
                        "Outstanding authentication operations prevent restart"
                );
            }

            log.info(() ->
                    "[AuthenticationService][start] "
                            + "Starting authentication service"
                            + " | platformThreads=" + createPlatformThread
                            + " | maxInFlightRequests="
                            + maxInFlightRequests
            );

            ExecutorService newWorkers = null;

            ScheduledExecutorService newScheduler = null;

            try {

                // ---------------------------------------------
                // INITIALIZE WORKER EXECUTOR
                // ---------------------------------------------

                /*
                 * Uses the existing four-argument
                 * ThreadPool factory contract.
                 *
                 * ThreadPool supplies AdmissionExecutor
                 * for worker-level admission.
                 *
                 * requestPermits independently bounds
                 * complete authentication operations.
                 */
                newWorkers =
                        new ThreadPool().initializeAndGetThreadPool(
                                coreThreads,
                                maxThreads,
                                idleTimeout,
                                createPlatformThread
                        );

                // ---------------------------------------------
                // INITIALIZE HOUSEKEEPING SCHEDULER
                // ---------------------------------------------

                newScheduler =
                        Executors.newSingleThreadScheduledExecutor(
                                runnable -> {

                                    Thread thread = new Thread(
                                            runnable,
                                            "authentication-housekeeping"
                                    );

                                    /*
                                     * A daemon housekeeping thread must
                                     * not independently keep the JVM alive.
                                     */
                                    thread.setDaemon(true);

                                    return thread;
                                }
                        );

                newScheduler.scheduleWithFixedDelay(
                        () -> {

                            try {

                                cleanup();

                            } catch (Exception exception) {

                                cleanupFailures.incrementAndGet();

                                log.error(
                                        "[AuthenticationService][scheduler] "
                                                + "Scheduled housekeeping failed",
                                        exception
                                );
                            }
                        },
                        CLEANUP_INTERVAL_SECONDS,
                        CLEANUP_INTERVAL_SECONDS,
                        TimeUnit.SECONDS
                );

                // ---------------------------------------------
                // PUBLISH SERVICE RESOURCES
                // ---------------------------------------------

                executorService = newWorkers;

                scheduledExecutorService = newScheduler;

                startedAt = Instant.now();

                staleRequests.set(0);

                isQuiescing = false;

                log.info(
                        "[AuthenticationService][start] "
                                + "Authentication service started successfully"
                );

            } catch (RuntimeException exception) {

                // ---------------------------------------------
                // ROLLBACK PARTIAL INITIALIZATION
                // ---------------------------------------------

                isQuiescing = true;

                if (newScheduler != null) {
                    newScheduler.shutdownNow();
                }

                if (newWorkers != null) {
                    newWorkers.shutdownNow();
                }

                log.error(
                        "[AuthenticationService][start] "
                                + "Authentication service startup failed",
                        exception
                );

                throw exception;
            }
        }
    }

    // =========================================================
    // 14. QUIESCE
    // =========================================================

    /**
     * Disables admission of new authentication requests.
     *
     * Already admitted operations remain registered
     * until terminal completion or failure.
     */
    @Override
    public void quiesce() {

        synchronized (admissionLock) {

            if (isQuiescing) {
                return;
            }

            isQuiescing = true;

            log.info(() ->
                    "[AuthenticationService][quiesce] "
                            + "Authentication admission disabled"
                            + " | outstandingRequests="
                            + outstandingRequests.get()
            );
        }
    }

    // =========================================================
    // 15. DRAIN
    // =========================================================

    /**
     * Waits for all admitted authentication operations
     * to reach terminal completion.
     *
     * Unlike worker-only draining, this also observes
     * operations waiting for asynchronous database replies.
     *
     * @param timeout Maximum drain duration in milliseconds.
     */
    @Override
    public void drain(
            long timeout
    ) {

        if (timeout < 0) {

            throw new IllegalArgumentException(
                    "Drain timeout cannot be negative"
            );
        }

        quiesce();

        long timeoutNanos =
                TimeUnit.MILLISECONDS.toNanos(timeout);

        long startNanos =
                System.nanoTime();

        log.info(() ->
                "[AuthenticationService][drain] "
                        + "Authentication drain started"
                        + " | timeoutMillis=" + timeout
                        + " | outstandingRequests="
                        + outstandingRequests.get()
        );

        while (hasOutstandingWork()) {

            long elapsed =
                    System.nanoTime() - startNanos;

            if (elapsed >= timeoutNanos) {

                log.warn(
                        "[AuthenticationService][drain] "
                                + "Drain deadline reached"
                                + " | outstandingRequests="
                                + outstandingRequests.get()
                );

                return;
            }

            if (Thread.currentThread().isInterrupted()) {

                log.warn(
                        "[AuthenticationService][drain] "
                                + "Drain interrupted"
                );

                return;
            }

            long remaining =
                    timeoutNanos - elapsed;

            LockSupport.parkNanos(
                    Math.min(
                            remaining,
                            TimeUnit.MILLISECONDS.toNanos(25)
                    )
            );
        }

        log.info(
                "[AuthenticationService][drain] "
                        + "All authentication operations completed"
        );
    }

    // =========================================================
    // 16. SHUTDOWN
    // =========================================================

    /**
     * Performs controlled service shutdown.
     *
     * Sequence:
     *
     * 1. Disable admission.
     * 2. Drain outstanding authentication operations.
     * 3. Shut down worker and housekeeping executors.
     * 4. Cancel queued tasks if worker termination fails.
     *
     * Already-running asynchronous handlers may continue
     * after the worker executor has terminated.
     *
     * The Platform lifecycle must coordinate dependent
     * MessageService and DatabaseService shutdown.
     */
    @Override
    public void shutdown() {

        quiesce();

        drain(SHUTDOWN_TIMEOUT_MILLIS);

        ExecutorService workers;
        ScheduledExecutorService scheduler;

        // -----------------------------------------------------
        // INITIATE EXECUTOR SHUTDOWN
        // -----------------------------------------------------

        synchronized (admissionLock) {

            workers = executorService;

            scheduler = scheduledExecutorService;

            if (workers != null) {
                workers.shutdown();
            }

            if (scheduler != null) {
                scheduler.shutdown();
            }
        }

        boolean interrupted = false;

        // -----------------------------------------------------
        // WAIT FOR WORKER TERMINATION
        // -----------------------------------------------------

        if (workers != null) {

            try {

                if (!workers.awaitTermination(
                        SHUTDOWN_TIMEOUT_MILLIS,
                        TimeUnit.MILLISECONDS
                )) {

                    log.warn(
                            "[AuthenticationService][shutdown] "
                                    + "Worker executor did not terminate within deadline"
                    );

                    cancelQueuedTasks(
                            workers.shutdownNow()
                    );
                }

            } catch (InterruptedException exception) {

                interrupted = true;

                cancelQueuedTasks(
                        workers.shutdownNow()
                );
            }
        }

        // -----------------------------------------------------
        // WAIT FOR HOUSEKEEPING TERMINATION
        // -----------------------------------------------------

        if (scheduler != null) {

            try {

                if (!scheduler.awaitTermination(
                        SHUTDOWN_TIMEOUT_MILLIS,
                        TimeUnit.MILLISECONDS
                )) {

                    scheduler.shutdownNow();
                }

            } catch (InterruptedException exception) {

                interrupted = true;

                scheduler.shutdownNow();
            }
        }

        // -----------------------------------------------------
        // REPORT SHUTDOWN STATE
        // -----------------------------------------------------

        if (hasOutstandingWork()) {

            log.warn(
                    "[AuthenticationService][shutdown] "
                            + "Service executors stopped with outstanding authentication operations"
                            + " | outstandingRequests="
                            + outstandingRequests.get()
            );

        } else {

            log.info(
                    "[AuthenticationService][shutdown] "
                            + "Authentication service stopped successfully"
            );
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    // =========================================================
    // 17. CANCEL QUEUED WORK
    // =========================================================

    /**
     * Releases authentication requests that never
     * started before forced executor shutdown.
     *
     * AdmissionExecutor returns original queued
     * Runnable objects during shutdownNow().
     */
    private void cancelQueuedTasks(
            List<Runnable> queuedTasks
    ) {

        int cancelled = 0;

        for (Runnable runnable : queuedTasks) {

            if (runnable instanceof AuthenticationTask task) {

                long before =
                        cancelledTasks.get();

                task.cancelBeforeExecution();

                if (cancelledTasks.get() > before) {
                    cancelled++;
                }
            }
        }

        final int totalCancelled = cancelled;

        log.debug(() ->
                "[AuthenticationService][cancelQueuedTasks] "
                        + "Queued authentication cancellation completed"
                        + " | cancelled=" + totalCancelled
        );
    }

    // =========================================================
    // 18. OUTSTANDING WORK
    // =========================================================

    /**
     * Returns true while authentication operations
     * remain unfinished.
     */
    private boolean hasOutstandingWork() {

        return outstandingRequests.get() > 0;
    }

    // =========================================================
    // 19. HOUSEKEEPING
    // =========================================================

    /**
     * Performs scheduled maintenance.
     *
     * Responsibilities:
     *
     * 1. Remove expired sessions when session management
     *    is configured.
     *
     * 2. Identify authentication operations that exceed
     *    the stale-request diagnostic threshold.
     *
     * Stale operations are not automatically removed
     * because the underlying asynchronous workflow may
     * still be executing.
     */
    @Override
    public void cleanup() {

        // -----------------------------------------------------
        // CLEANUP EXPIRED SESSIONS
        // -----------------------------------------------------

        if (sessionManager != null) {

            sessionManager.cleanupExpiredSessions();
        }

        // -----------------------------------------------------
        // DETECT STALE AUTHENTICATION REQUESTS
        // -----------------------------------------------------

        int staleCount = 0;

        for (AuthenticationTask task :
                pendingRequests.values()) {

            if (task.ageSeconds() >= STALE_REQUEST_SECONDS) {
                staleCount++;
            }
        }

        staleRequests.set(staleCount);

        // -----------------------------------------------------
        // REPORT STALE REQUESTS
        // -----------------------------------------------------

        if (staleCount > 0) {

            long warningCount =
                    staleWarningCount.incrementAndGet();

            if (isPowerOfTwo(warningCount)) {

                final int currentStaleCount =
                        staleCount;

                log.warn(() ->
                        "[AuthenticationService][cleanup] "
                                + "Long-running authentication operations detected"
                                + " | staleRequests="
                                + currentStaleCount
                                + " | outstandingRequests="
                                + outstandingRequests.get()
                );
            }

        } else {

            staleWarningCount.set(0);

            log.debug(() ->
                    "[AuthenticationService][cleanup] "
                            + "Authentication housekeeping completed"
                            + " | pendingRequests="
                            + pendingRequests.size()
            );
        }
    }

    // =========================================================
    // 20. WORKER SUBMISSION
    // =========================================================

    /**
     * Submits an already-admitted request to the
     * underlying executor.
     *
     * No additional quiescence check is performed.
     *
     * Caller must hold admissionLock and complete
     * service admission before invoking this method.
     */
    private void submitToThreadPool(
            Runnable runnable
    ) {

        Objects.requireNonNull(
                runnable,
                "Runnable cannot be null"
        );

        /*
         * execute() avoids an unnecessary FutureTask.
         *
         * Response completion remains the responsibility
         * of MessageService and its ReplyChannels.
         */
        executorService.execute(runnable);
    }

    // =========================================================
    // 21. METRICS
    // =========================================================

    /**
     * Returns a diagnostic snapshot of service metrics.
     *
     * Counter values are sampled concurrently and
     * do not constitute a globally atomic snapshot.
     */
    public Map<String, Object> metrics() {

        ExecutorService workers =
                executorService;

        ScheduledExecutorService scheduler =
                scheduledExecutorService;

        Map<String, Object> metrics =
                new LinkedHashMap<>();

        // -----------------------------------------------------
        // SERVICE STATE
        // -----------------------------------------------------

        metrics.put(
                "isQuiescing",
                isQuiescing
        );

        metrics.put(
                "startedAt",
                startedAt == null
                        ? "NOT_STARTED"
                        : startedAt.toString()
        );

        // -----------------------------------------------------
        // EXECUTOR CONFIGURATION
        // -----------------------------------------------------

        metrics.put(
                "platformThreads",
                createPlatformThread
        );

        metrics.put(
                "coreThreads",
                coreThreads
        );

        metrics.put(
                "maxThreads",
                maxThreads
        );

        metrics.put(
                "idleTimeoutMillis",
                idleTimeout
        );

        metrics.put(
                "maxInFlightRequests",
                maxInFlightRequests
        );

        metrics.put(
                "availableRequestPermits",
                requestPermits.availablePermits()
        );

        // -----------------------------------------------------
        // REQUEST COUNTERS
        // -----------------------------------------------------

        metrics.put(
                "submittedTasks",
                submittedTasks.get()
        );

        metrics.put(
                "completedRequests",
                completedRequests.get()
        );

        metrics.put(
                "errorResponses",
                errorResponses.get()
        );

        metrics.put(
                "failedRequests",
                failedRequests.get()
        );

        metrics.put(
                "rejectedRequests",
                rejectedRequests.get()
        );

        metrics.put(
                "cancelledTasks",
                cancelledTasks.get()
        );

        // -----------------------------------------------------
        // CURRENT ACTIVITY
        // -----------------------------------------------------

        metrics.put(
                "activeWorkerTasks",
                activeWorkerTasks.get()
        );

        metrics.put(
                "outstandingRequests",
                outstandingRequests.get()
        );

        metrics.put(
                "pendingRequests",
                pendingRequests.size()
        );

        metrics.put(
                "staleRequests",
                staleRequests.get()
        );

        metrics.put(
                "cleanupFailures",
                cleanupFailures.get()
        );

        // -----------------------------------------------------
        // EXECUTOR STATUS
        // -----------------------------------------------------

        metrics.put(
                "executorInitialized",
                workers != null
        );

        metrics.put(
                "executorShutdown",
                workers != null && workers.isShutdown()
        );

        metrics.put(
                "executorTerminated",
                workers != null && workers.isTerminated()
        );

        metrics.put(
                "schedulerInitialized",
                scheduler != null
        );

        metrics.put(
                "schedulerShutdown",
                scheduler != null && scheduler.isShutdown()
        );

        metrics.put(
                "schedulerTerminated",
                scheduler != null && scheduler.isTerminated()
        );

        // -----------------------------------------------------
        // WORKER ADMISSION METRICS
        // -----------------------------------------------------

        if (workers != null) {

            metrics.putAll(
                    ThreadPool.admissionMetrics(workers)
            );
        }

        // -----------------------------------------------------
        // SESSION MANAGER METRICS
        // -----------------------------------------------------

        if (sessionManager != null) {

            metrics.put(
                    "sessionMetrics",
                    sessionManager.metrics()
            );
        }

        return Map.copyOf(metrics);
    }

    // =========================================================
    // 22. HEALTH CHECK
    // =========================================================

    /**
     * Evaluates authentication service health.
     *
     * Health is determined using:
     *
     * - Executor availability.
     * - Scheduler availability.
     * - Admission state.
     * - Stale authentication operations.
     * - Request failures.
     * - Request rejections.
     * - Housekeeping failures.
     *
     * Business-level authentication denials are not
     * automatically classified as infrastructure failures.
     */
    @Override
    public HealthReport checkHealth() {

        ExecutorService workers =
                executorService;

        ScheduledExecutorService scheduler =
                scheduledExecutorService;

        HealthStatus status;
        String description;

        // -----------------------------------------------------
        // NOT INITIALIZED
        // -----------------------------------------------------

        if (workers == null || scheduler == null) {

            status = HealthStatus.UNREACHABLE;

            description =
                    "Authentication service has not been initialized.";

            // -----------------------------------------------------
            // EXECUTOR FAILURE
            // -----------------------------------------------------

        } else if (!isQuiescing
                && (workers.isShutdown()
                || scheduler.isShutdown())) {

            status = HealthStatus.UNHEALTHY;

            description =
                    "Authentication executor or scheduler is unavailable.";

            // -----------------------------------------------------
            // QUIESCING
            // -----------------------------------------------------

        } else if (isQuiescing) {

            status = HealthStatus.DEGRADED;

            description =
                    "Authentication service is not accepting new requests.";

            // -----------------------------------------------------
            // OPERATIONAL
            // -----------------------------------------------------

        } else if (staleRequests.get() > 0
                || cleanupFailures.get() > 0
                || failedRequests.get() > 0
                || rejectedRequests.get() > 0) {

            status = HealthStatus.DEGRADED;

            description =
                    "Authentication service is operational with recorded execution or admission problems.";

        } else {

            status = HealthStatus.HEALTHY;

            description =
                    "Authentication service is operating normally.";
        }

        final HealthStatus finalStatus = status;

        log.debug(() ->
                "[AuthenticationService][checkHealth] "
                        + "Health evaluation completed"
                        + " | status=" + finalStatus
                        + " | activeWorkerTasks="
                        + activeWorkerTasks.get()
                        + " | outstandingRequests="
                        + outstandingRequests.get()
                        + " | staleRequests="
                        + staleRequests.get()
        );

        return new HealthReport(
                SERVICE_NAME,
                status,
                description,
                Instant.now(),
                metrics()
        );
    }

    // =========================================================
    // 23. SERVICE IDENTITY
    // =========================================================

    @Override
    public String getName() {

        return SERVICE_NAME;
    }

    // =========================================================
    // 24. INTERNAL UTILITIES
    // =========================================================

    /**
     * Checks whether a positive integer is a power of two.
     *
     * Used for reducing repetitive warning logs
     * during sustained rejection bursts.
     */
    private static boolean isPowerOfTwo(
            long value
    ) {

        return value > 0
                && (value & (value - 1)) == 0;
    }
}
