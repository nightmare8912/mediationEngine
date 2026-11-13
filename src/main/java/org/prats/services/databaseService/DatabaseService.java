
package org.prats.services.databaseService;

import org.prats.database.databaseUtilities.DatabaseResourceManager;
import org.prats.definitions.interfaces.HealthCheckable;
import org.prats.definitions.interfaces.Submittable;
import org.prats.definitions.records.HealthReport;
import org.prats.definitions.enums.HealthStatus;
import org.prats.definitions.classes.Message;
import org.prats.loggers.FileLogger;
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
        this.coreThreads                = coreThreads;
        this.maxThreads                 = maxThreads;
        this.idleTimeout                = idleTimeout;
        this.isQuiescing                = true;
        this.databaseRequestHandler     = databaseRequestHandler;

        this.databaseResourceManager    = databaseResourceManager;
        this.admissionLock              = new Object();
    }


    @Override
    public void start() {

        synchronized (lifecycleLock) {

            synchronized (admissionLock) {
                isQuiescing = true;
            }
            ExecutorService previousWorkers = databaseWorkers;

            if (previousWorkers != null && !previousWorkers.isTerminated()) {
                throw new IllegalStateException(
                        "Cannot start DatabaseService: previous worker executor is still operational"
                );
            }

            DataSource newDataSource = null;
            ExecutorService newWorkers = null;

            try {

                FileLogger.log("[DatabaseService] Initializing database resources...");

                databaseResourceManager.initialize();

                FileLogger.log("[DatabaseService] Database resources initialized.");

                newDataSource = Objects.requireNonNull(
                        databaseResourceManager.getDataSource(),
                        "Database DataSource was not initialized"
                );

                FileLogger.log("[DatabaseService] DataSource acquired.");

                newWorkers = Objects.requireNonNull(
                        new ThreadPool().initializeAndGetThreadPool(
                                coreThreads,
                                maxThreads,
                                idleTimeout,
                                false
                        ),
                        "Database worker executor initialization returned null"
                );

                FileLogger.log("[DatabaseService] Worker executor initialized.");

                synchronized (admissionLock) {
                    dataSource = newDataSource;
                    databaseWorkers = newWorkers;
                    isQuiescing = false;
                }

                lastStartupFailure = null;
                FileLogger.log("[DatabaseService] Startup completed.");

            } catch (Exception exception) {

                synchronized (admissionLock) {
                    isQuiescing = true;
                }

                if (newWorkers != null) {
                    newWorkers.shutdownNow();
                }
                try {
                    databaseResourceManager.close();
                } catch (RuntimeException cleanupException) {
                    exception.addSuppressed(cleanupException);
                }

                synchronized (admissionLock) {
                    databaseWorkers = null;
                    dataSource = null;
                    isQuiescing = true;
                }

                lastStartupFailure = exception.toString();
                FileLogger.log(
                        "[DatabaseService] Startup failed: " + exception
                );
                exception.printStackTrace();
                throw new IllegalStateException(
                        "DatabaseService initialization failed",
                        exception
                );
            }
        }
    }



    @Override
    public void shutdown() {

        synchronized (lifecycleLock) {

            synchronized (admissionLock) {
                isQuiescing = true;
            }

            ExecutorService workers = databaseWorkers;
            if (workers != null) {
                workers.shutdown();
                try {
                    boolean terminated = workers.awaitTermination(
                            SHUTDOWN_GRACE_SECONDS,
                            TimeUnit.SECONDS
                    );
                    if (!terminated) {

                        FileLogger.log(
                                "[DatabaseService] Graceful shutdown timed out. Forcing shutdown."
                        );
                        workers.shutdownNow();
                        terminated = workers.awaitTermination(
                                SHUTDOWN_FORCE_SECONDS,
                                TimeUnit.SECONDS
                        );
                    }
                    if (!terminated) {
                        throw new IllegalStateException(
                                "Database workers did not terminate within the shutdown deadline"
                        );
                    }
                } catch (InterruptedException exception) {
                    workers.shutdownNow();
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Interrupted while shutting down DatabaseService",
                            exception
                    );
                }
            }

            databaseResourceManager.close();

            synchronized (admissionLock) {
                databaseWorkers = null;
                dataSource = null;
                isQuiescing = true;
            }
            FileLogger.log("[DatabaseService] Shutdown completed.");
        }
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
        return outstandingTasks.get() > 0;
    }


    @Override
    public void submit(Message message) {
        synchronized (admissionLock) {
            if (isQuiescing) {
                rejectedDuringQuiesce.incrementAndGet();
                return;
            }

            ExecutorService workers = databaseWorkers;
            if (workers == null || workers.isShutdown()) {
                rejectedTasks.incrementAndGet();
                return;
            }
            submitToThreadPool(
                    workers,
                    () -> {
                        messageDispatcher.dispatchResponse(
                                databaseRequestHandler.handleRequest(message)
                        );
                    }
            );
        }
    }


    // centralized the submit task to capture diagnostic at one place
    private void submitToThreadPool(
            ExecutorService workers,
            Runnable runnable
    ) {

        outstandingTasks.incrementAndGet();
        submittedTasks.incrementAndGet();

        try {
            workers.execute(() -> {
                activeTasks.incrementAndGet();
                try {
                    runnable.run();
                } catch (RuntimeException | Error exception) {
                    failedTasks.incrementAndGet();
                    throw exception;
                } finally {
                    completedTasks.incrementAndGet();
                    activeTasks.decrementAndGet();
                    outstandingTasks.decrementAndGet();
                }
            });
        } catch (RejectedExecutionException exception) {
            outstandingTasks.decrementAndGet();
            submittedTasks.decrementAndGet();
            rejectedTasks.incrementAndGet();
            throw exception;
        }
    }


    public void setMessageDispatcher(MessageDispatcher messageDispatcher) {
        this.messageDispatcher = messageDispatcher;
    }


    @Override
    public synchronized HealthReport checkHealth() {

        Map<String, Object> metrics = new LinkedHashMap<>();

        ExecutorService workers = databaseWorkers;
        DataSource currentDataSource = dataSource;

        boolean executorInitialized = workers != null;
        boolean dataSourceInitialized = currentDataSource != null;
        boolean executorShutdown =
                workers != null && workers.isShutdown();
        boolean executorTerminated =
                workers != null && workers.isTerminated();

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
        } else if (executorShutdown || executorTerminated) {
            status = HealthStatus.UNHEALTHY;
            message = "Database worker executor is no longer operational.";

        } else {

            boolean databaseConnectionAvailable =
                    checkDatabaseConnection(currentDataSource);
            metrics.put(
                    "databaseConnectionAvailable",
                    databaseConnectionAvailable
            );
            if (!databaseConnectionAvailable) {
                status = HealthStatus.UNHEALTHY;
                message = "Unable to establish a database connection.";

            } else {
                boolean newTaskFailures =
                        failedTasks.get() > lastReportedFailedTasks;
                boolean newTaskRejections =
                        rejectedTasks.get() > lastReportedRejectedTasks;
                lastReportedFailedTasks = failedTasks.get();
                lastReportedRejectedTasks = rejectedTasks.get();
                if (newTaskFailures || newTaskRejections) {

                    status = HealthStatus.DEGRADED;

                    message = "Database service is operational but has experienced task failures or rejections.";

                } else {

                    status = HealthStatus.HEALTHY;

                    message = "Database service is operating normally.";
                }
            }
        }
        return new HealthReport(
                "DatabaseService",
                status,
                message,
                Instant.now(),
                metrics
        );
    }


    private boolean checkDatabaseConnection(DataSource source) {
        try (Connection connection = source.getConnection()) {
            return connection.isValid(2);
        } catch (SQLException exception) {
            connectionAcquisitionFailures.incrementAndGet();
            return false;
        }
    }

}
