
package org.prats.services.databaseService;

import org.prats.database.databaseUtilities.DatabaseResourceManager;
import org.prats.definitions.interfaces.HealthCheckable;
import org.prats.definitions.interfaces.Submittable;
import org.prats.definitions.records.HealthReport;
import org.prats.definitions.enums.HealthStatus;
import org.prats.definitions.classes.Message;
import org.prats.loggers.EngineLogger;
import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.handlers.DatabaseRequestHandler;
import org.prats.definitions.interfaces.Service;
import org.prats.threading.ThreadPool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class DatabaseService implements Service, Submittable, HealthCheckable {

    private static final EngineLogger log =
            EngineLogger.getLogger(DatabaseService.class);

    private final Object admissionLock;
    private final DatabaseResourceManager databaseResourceManager;
    private final DatabaseRequestHandler databaseRequestHandler;

    private final int coreThreads;
    private final int maxThreads;
    private final long idleTimeout;

    private final Object lifecycleLock = new Object();

    private volatile DataSource dataSource;
    private volatile ExecutorService databaseWorkers;

    private volatile String lastStartupFailure;

    private static final long SHUTDOWN_GRACE_SECONDS = 30;
    private static final long SHUTDOWN_FORCE_SECONDS = 5;

    private MessageDispatcher messageDispatcher;

    private volatile boolean isQuiescing;

    private final AtomicLong submittedTasks = new AtomicLong();
    private final AtomicLong completedTasks = new AtomicLong();
    private final AtomicLong failedTasks = new AtomicLong();
    private final AtomicLong rejectedTasks = new AtomicLong();
    private final AtomicLong rejectedDuringQuiesce = new AtomicLong();
    private final AtomicLong connectionAcquisitionFailures = new AtomicLong();

    private final AtomicInteger activeTasks = new AtomicInteger();
    private final AtomicInteger outstandingTasks = new AtomicInteger();

    private long lastReportedFailedTasks;
    private long lastReportedRejectedTasks;

    public DatabaseService(
            int coreThreads,
            int maxThreads,
            long idleTimeout,
            DatabaseResourceManager databaseResourceManager,
            DatabaseRequestHandler databaseRequestHandler
    ) {

        log.debug(() ->
                "[DatabaseService][constructor] Initializing DatabaseService"
                        + " | coreThreads=" + coreThreads
                        + " | maxThreads=" + maxThreads
                        + " | idleTimeout=" + idleTimeout
        );

        this.coreThreads                = coreThreads;
        this.maxThreads                 = maxThreads;
        this.idleTimeout                = idleTimeout;
        this.isQuiescing                = true;
        this.databaseRequestHandler     = databaseRequestHandler;

        this.databaseResourceManager    = databaseResourceManager;
        this.admissionLock              = new Object();

        log.info("[DatabaseService][constructor] DatabaseService initialized");
    }


    @Override
    public void start() {

        log.info("[DatabaseService][start] DatabaseService startup requested");

        synchronized (lifecycleLock) {

            log.debug("[DatabaseService][start] Acquired lifecycle lock");

            synchronized (admissionLock) {

                isQuiescing = true;

                log.debug("[DatabaseService][start] Task admission disabled during initialization");
            }

            ExecutorService previousWorkers = databaseWorkers;

            if (previousWorkers != null && !previousWorkers.isTerminated()) {

                log.error(
                        "[DatabaseService][start] Cannot start DatabaseService: previous worker executor is still operational"
                );

                throw new IllegalStateException(
                        "Cannot start DatabaseService: previous worker executor is still operational"
                );
            }

            DataSource newDataSource = null;
            ExecutorService newWorkers = null;

            try {

                log.info("[DatabaseService][start] Initializing database resources...");

                log.debug("[DatabaseService][start] Invoking DatabaseResourceManager.initialize()");

                databaseResourceManager.initialize();

                log.info("[DatabaseService][start] Database resources initialized.");

                log.debug("[DatabaseService][start] Acquiring DataSource");

                newDataSource = Objects.requireNonNull(
                        databaseResourceManager.getDataSource(),
                        "Database DataSource was not initialized"
                );

                log.info("[DatabaseService][start] DataSource acquired.");

                log.debug(() ->
                        "[DatabaseService][start] Initializing database worker executor"
                                + " | coreThreads=" + coreThreads
                                + " | maxThreads=" + maxThreads
                                + " | idleTimeout=" + idleTimeout
                );

                newWorkers = Objects.requireNonNull(
                        new ThreadPool().initializeAndGetThreadPool(
                                coreThreads,
                                maxThreads,
                                idleTimeout,
                                false
                        ),
                        "Database worker executor initialization returned null"
                );

                log.info("[DatabaseService][start] Worker executor initialized.");

                synchronized (admissionLock) {

                    log.debug("[DatabaseService][start] Publishing initialized database resources");

                    dataSource = newDataSource;
                    databaseWorkers = newWorkers;
                    isQuiescing = false;

                    log.debug("[DatabaseService][start] Task admission enabled");
                }

                lastStartupFailure = null;

                log.info("[DatabaseService][start] Startup completed.");

            } catch (Exception exception) {

                log.error(
                        "[DatabaseService][start] DatabaseService startup failed",
                        exception
                );

                synchronized (admissionLock) {

                    isQuiescing = true;

                    log.debug("[DatabaseService][start] Task admission disabled following startup failure");
                }

                if (newWorkers != null) {

                    log.debug("[DatabaseService][start] Shutting down partially initialized worker executor");

                    newWorkers.shutdownNow();
                }

                try {

                    log.debug("[DatabaseService][start] Closing partially initialized database resources");

                    databaseResourceManager.close();

                } catch (RuntimeException cleanupException) {

                    exception.addSuppressed(cleanupException);

                    log.error(
                            "[DatabaseService][start] Database resource cleanup failed after startup failure",
                            cleanupException
                    );
                }

                synchronized (admissionLock) {

                    databaseWorkers = null;
                    dataSource = null;
                    isQuiescing = true;

                    log.debug("[DatabaseService][start] Cleared partially initialized database resources");
                }

                lastStartupFailure = exception.toString();

                log.error(
                        "[DatabaseService] Startup failed: " + exception
                );

                throw new IllegalStateException(
                        "DatabaseService initialization failed",
                        exception
                );
            }
        }
    }



    @Override
    public void shutdown() {

        log.info("[DatabaseService][shutdown] DatabaseService shutdown requested");

        synchronized (lifecycleLock) {

            log.debug("[DatabaseService][shutdown] Acquired lifecycle lock");

            synchronized (admissionLock) {

                isQuiescing = true;

                log.info("[DatabaseService][shutdown] New task admission disabled");
            }

            ExecutorService workers = databaseWorkers;

            if (workers != null) {

                log.debug("[DatabaseService][shutdown] Initiating graceful worker executor shutdown");

                workers.shutdown();

                try {

                    log.debug(() ->
                            "[DatabaseService][shutdown] Waiting for worker termination"
                                    + " | gracePeriodSeconds=" + SHUTDOWN_GRACE_SECONDS
                    );

                    boolean terminated = workers.awaitTermination(
                            SHUTDOWN_GRACE_SECONDS,
                            TimeUnit.SECONDS
                    );

                    if (!terminated) {

                        log.warn(
                                "[DatabaseService][shutdown] Graceful shutdown timed out. Forcing shutdown."
                        );

                        workers.shutdownNow();

                        log.warn(() ->
                                "[DatabaseService][shutdown] Waiting for forced worker termination"
                                        + " | forcePeriodSeconds=" + SHUTDOWN_FORCE_SECONDS
                        );

                        terminated = workers.awaitTermination(
                                SHUTDOWN_FORCE_SECONDS,
                                TimeUnit.SECONDS
                        );
                    }

                    if (!terminated) {

                        log.error(
                                "[DatabaseService][shutdown] Database workers did not terminate within the shutdown deadline"
                        );

                        throw new IllegalStateException(
                                "Database workers did not terminate within the shutdown deadline"
                        );
                    }

                    log.info("[DatabaseService][shutdown] Database worker executor terminated successfully");

                } catch (InterruptedException exception) {

                    log.error(
                            "[DatabaseService][shutdown] Interrupted while waiting for database worker termination",
                            exception
                    );

                    workers.shutdownNow();

                    Thread.currentThread().interrupt();

                    throw new IllegalStateException(
                            "Interrupted while shutting down DatabaseService",
                            exception
                    );
                }
            }

            log.debug("[DatabaseService][shutdown] Closing database resources");

            databaseResourceManager.close();

            synchronized (admissionLock) {

                databaseWorkers = null;
                dataSource = null;
                isQuiescing = true;

                log.debug("[DatabaseService][shutdown] Database resource references cleared");
            }

            log.info("[DatabaseService][shutdown] Shutdown completed.");
        }
    }


    @Override
    public void quiesce() {

        log.info("[DatabaseService][quiesce] DatabaseService quiescence requested");

        synchronized (admissionLock) {

            isQuiescing = true;
        }

        log.debug("[DatabaseService][quiesce] Task admission disabled");

        log.info("[DatabaseService][quiesce] DatabaseService is now quiescing");
    }

    @Override
    public void drain(long timeout) {

        log.info(() ->
                "[DatabaseService][drain] DatabaseService drain requested"
                        + " | timeoutMillis=" + timeout
                        + " | outstandingTasks=" + outstandingTasks.get()
        );

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

        int remainingTasks = outstandingTasks.get();

        if (remainingTasks > 0) {

            log.warn(() ->
                    "[DatabaseService][drain] Drain deadline reached with outstanding work remaining"
                            + " | outstandingTasks=" + remainingTasks
                            + " | timeoutMillis=" + timeout
            );

        } else {

            log.info("[DatabaseService][drain] All outstanding database tasks drained successfully");
        }
    }

    @Override
    public void cleanup() {

        log.debug("[DatabaseService][cleanup] Cleanup invoked");
    }

    private boolean hasOutstandingWork() {

        return outstandingTasks.get() > 0;
    }


    @Override
    public void submit(Message message) {

        log.debug("[DatabaseService][submit] Database request submission received");

        synchronized (admissionLock) {

            if (isQuiescing) {

                long rejectedCount = rejectedDuringQuiesce.incrementAndGet();

                log.warn(() ->
                        "[DatabaseService][submit] Request rejected because DatabaseService is quiescing"
                                + " | rejectedDuringQuiesce=" + rejectedCount
                );

                messageDispatcher.dispatchResponse(databaseRequestHandler.error(message, "DATABASE_UNAVAILABLE", "Database service is quiescing."));
                return;
            }

            ExecutorService workers = databaseWorkers;

            if (workers == null || workers.isShutdown()) {

                long rejectedCount = rejectedTasks.incrementAndGet();

                log.warn(() ->
                        "[DatabaseService][submit] Request rejected because database workers are unavailable"
                                + " | executorInitialized=" + (workers != null)
                                + " | executorShutdown="
                                + (workers != null && workers.isShutdown())
                                + " | rejectedTasks=" + rejectedCount
                );

                messageDispatcher.dispatchResponse(databaseRequestHandler.error(message, "DATABASE_UNAVAILABLE", "Database workers are unavailable."));
                return;
            }

            log.debug(() ->
                    "[DatabaseService][submit] Database request accepted for execution"
                            + " | activeTasks=" + activeTasks.get()
                            + " | outstandingTasks=" + outstandingTasks.get()
            );

            try { submitToThreadPool(
                    workers,
                    () -> {

                        log.debug("[DatabaseService][submit] Processing database request");

                        messageDispatcher.dispatchResponse(
                                databaseRequestHandler.handleRequest(message)
                        );

                        log.debug("[DatabaseService][submit] Database request processing completed");
                    }
            ); } catch (RejectedExecutionException exception) {
                messageDispatcher.dispatchResponse(databaseRequestHandler.error(message, "OVERLOADED", "Database service is overloaded. Retry later."));
            }
        }
    }


    // centralized the submit task to capture diagnostic at one place
    private void submitToThreadPool(
            ExecutorService workers,
            Runnable runnable
    ) {

        log.debug("[DatabaseService][submitToThreadPool] Preparing task submission");

        long currentOutstanding = outstandingTasks.incrementAndGet();
        long currentSubmitted = submittedTasks.incrementAndGet();

        log.debug(() ->
                "[DatabaseService][submitToThreadPool] Task counters incremented"
                        + " | submittedTasks=" + currentSubmitted
                        + " | outstandingTasks=" + currentOutstanding
                        + " | activeTasks=" + activeTasks.get()
        );

        try {

            workers.execute(() -> {

                int currentActive = activeTasks.incrementAndGet();

                log.debug(() ->
                        "[DatabaseService][submitToThreadPool] Worker task started"
                                + " | activeTasks=" + currentActive
                                + " | outstandingTasks=" + outstandingTasks.get()
                );

                try {

                    runnable.run();

                    log.debug("[DatabaseService][submitToThreadPool] Worker task completed execution");

                } catch (RuntimeException | Error exception) {

                    long currentFailed = failedTasks.incrementAndGet();

                    log.error(
                            "[DatabaseService][submitToThreadPool] Database worker task failed"
                                    + " | failedTasks=" + currentFailed,
                            exception
                    );

                    throw exception;

                } finally {

                    long currentCompleted = completedTasks.incrementAndGet();
                    int remainingActive = activeTasks.decrementAndGet();
                    int remainingOutstanding = outstandingTasks.decrementAndGet();

                    log.debug(() ->
                            "[DatabaseService][submitToThreadPool] Worker task metrics updated"
                                    + " | completedTasks=" + currentCompleted
                                    + " | activeTasks=" + remainingActive
                                    + " | outstandingTasks=" + remainingOutstanding
                    );
                }
            });

            log.debug("[DatabaseService][submitToThreadPool] Task accepted by worker executor");

        } catch (RejectedExecutionException exception) {

            outstandingTasks.decrementAndGet();
            submittedTasks.decrementAndGet();

            long currentRejected = rejectedTasks.incrementAndGet();

            log.warn(() ->
                    "[DatabaseService][submitToThreadPool] Worker executor rejected task"
                            + " | rejectedTasks=" + currentRejected
                            + " | outstandingTasks=" + outstandingTasks.get()
            );

            throw exception;
        }
    }


    public void setMessageDispatcher(MessageDispatcher messageDispatcher) {

        log.debug("[DatabaseService][setMessageDispatcher] Configuring message dispatcher");

        this.messageDispatcher = messageDispatcher;

        log.debug("[DatabaseService][setMessageDispatcher] Message dispatcher configured");
    }


    @Override
    public synchronized HealthReport checkHealth() {

        log.debug("[DatabaseService][checkHealth] Database health check started");

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.putAll(org.prats.threading.ThreadPool.admissionMetrics(databaseWorkers));

        ExecutorService workers = databaseWorkers;
        DataSource currentDataSource = dataSource;

        boolean executorInitialized = workers != null;
        boolean dataSourceInitialized = currentDataSource != null;
        boolean executorShutdown =
                workers != null && workers.isShutdown();
        boolean executorTerminated =
                workers != null && workers.isTerminated();

        log.debug(() ->
                "[DatabaseService][checkHealth] Captured database service state"
                        + " | executorInitialized=" + executorInitialized
                        + " | dataSourceInitialized=" + dataSourceInitialized
                        + " | executorShutdown=" + executorShutdown
                        + " | executorTerminated=" + executorTerminated
                        + " | isQuiescing=" + isQuiescing
        );

        metrics.put("isQuiescing", isQuiescing);
        metrics.put("dataSourceInitialized", dataSourceInitialized);
        metrics.put("executorInitialized", executorInitialized);
        metrics.put("executorShutdown", executorShutdown);
        metrics.put("executorTerminated", executorTerminated);
        metrics.put(
                "executorOperational",
                workers != null && !executorShutdown && !executorTerminated
        );

        metrics.put("lastStartupFailure", lastStartupFailure);
        metrics.put("submittedTasks", submittedTasks.get());
        metrics.put("completedTasks", completedTasks.get());
        metrics.put("failedTasks", failedTasks.get());
        metrics.put("rejectedTasks", rejectedTasks.get());
        metrics.put("rejectedDuringQuiesce", rejectedDuringQuiesce.get());
        metrics.put("activeTasks", activeTasks.get());
        metrics.put("outstandingTasks", outstandingTasks.get());
        metrics.put(
                "connectionAcquisitionFailures",
                connectionAcquisitionFailures.get()
        );

        HealthStatus status;
        String message;

        if (workers == null || currentDataSource == null) {

            status = HealthStatus.UNREACHABLE;

            message = "Database service has not been initialized.";

            log.warn("[DatabaseService][checkHealth] Database service has not been initialized");

        } else if (executorShutdown || executorTerminated) {

            status = HealthStatus.UNHEALTHY;

            message = "Database worker executor is no longer operational.";

            log.error("[DatabaseService][checkHealth] Database worker executor is no longer operational");

        } else {

            log.debug("[DatabaseService][checkHealth] Checking database connection");

            boolean databaseConnectionAvailable =
                    checkDatabaseConnection(currentDataSource);

            metrics.put(
                    "databaseConnectionAvailable",
                    databaseConnectionAvailable
            );

            if (!databaseConnectionAvailable) {

                status = HealthStatus.UNHEALTHY;

                message = "Unable to establish a database connection.";

                log.error("[DatabaseService][checkHealth] Unable to establish a database connection");

            } else {

                boolean newTaskFailures =
                        failedTasks.get() > lastReportedFailedTasks;

                boolean newTaskRejections =
                        rejectedTasks.get() > lastReportedRejectedTasks;

                log.debug(() ->
                        "[DatabaseService][checkHealth] Evaluating recent task failures and rejections"
                                + " | newTaskFailures=" + newTaskFailures
                                + " | newTaskRejections=" + newTaskRejections
                                + " | failedTasks=" + failedTasks.get()
                                + " | rejectedTasks=" + rejectedTasks.get()
                );

                lastReportedFailedTasks = failedTasks.get();
                lastReportedRejectedTasks = rejectedTasks.get();

                if (newTaskFailures || newTaskRejections) {

                    status = HealthStatus.DEGRADED;

                    message = "Database service is operational but has experienced task failures or rejections.";

                    log.warn("[DatabaseService][checkHealth] Database service is operational but degraded due to task failures or rejections");

                } else {

                    status = HealthStatus.HEALTHY;

                    message = "Database service is operating normally.";

                    log.debug("[DatabaseService][checkHealth] Database service is healthy");
                }
            }
        }

        log.info(() ->
                "[DatabaseService][checkHealth] Database health evaluation completed"
                        + " | status=" + status
                        + " | activeTasks=" + activeTasks.get()
                        + " | outstandingTasks=" + outstandingTasks.get()
                        + " | submittedTasks=" + submittedTasks.get()
                        + " | completedTasks=" + completedTasks.get()
                        + " | failedTasks=" + failedTasks.get()
                        + " | rejectedTasks=" + rejectedTasks.get()
        );

        return new HealthReport(
                "DatabaseService",
                status,
                message,
                Instant.now(),
                metrics
        );
    }


    private boolean checkDatabaseConnection(DataSource source) {

        log.debug("[DatabaseService][checkDatabaseConnection] Database connection validation started");

        try (Connection connection = source.getConnection()) {

            log.debug("[DatabaseService][checkDatabaseConnection] Database connection acquired; validating connection");

            boolean valid = connection.isValid(2);

            if (valid) {

                log.debug("[DatabaseService][checkDatabaseConnection] Database connection is valid");

            } else {

                log.warn("[DatabaseService][checkDatabaseConnection] Database connection validation returned false");
            }

            return valid;

        } catch (SQLException exception) {

            long failures = connectionAcquisitionFailures.incrementAndGet();

            log.warn(() ->
                    "[DatabaseService][checkDatabaseConnection] Database connection acquisition or validation failed"
                            + " | connectionAcquisitionFailures=" + failures
                            + " | exception=" + exception
            );

            return false;
        }
    }

}
