
package org.prats.services.databaseService;

import org.prats.database.databaseUtilities.DatabaseResourceManager;
import org.prats.definitions.HealthCheckable;
import org.prats.definitions.HealthReport;
import org.prats.definitions.HealthStatus;
import org.prats.definitions.Message;
import org.prats.services.messageService.MessageDispatcher;
import org.prats.handlers.DatabaseRequestHandler;
import org.prats.services.Service;
import org.prats.threading.ThreadPool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
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

    private DataSource dataSource;
    private MessageDispatcher messageDispatcher;
    private ExecutorService databaseWorkers;

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
        databaseResourceManager.initialize();
        dataSource  = databaseResourceManager.getDataSource();

        databaseWorkers = new ThreadPool().initializeAndGetThreadPool(
                coreThreads,
                maxThreads,
                idleTimeout,
                false
        );

        isQuiescing = false;
    }

    @Override
    public void shutdown() {
        databaseWorkers.shutdownNow();
        if (dataSource != null) {
            databaseResourceManager.close();
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

    private boolean hasOutstandingWork() {
        return outstandingTasks.get() > 0;
    }

    @Override
    public void submit(Message message) {
        synchronized (admissionLock) {
            if (!isQuiescing)
                submitToThreadPool(() -> {
                    messageDispatcher.dispatchResponse(databaseRequestHandler.handleRequest(message));
                });
            else {
                rejectedDuringQuiesce.incrementAndGet();
                // TODO: this is important, a message needs to be delivered back to the caller that the db service is not accepting work,
                // otherwise, the calling service would not know what happened with their request
            }
        }
    }

    public Connection getConnection() throws SQLException {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            connectionAcquisitionFailures.incrementAndGet();
            throw e;
        }
    }

    // centralized the submit task to capture diagnostic at one place
    private void submitToThreadPool(Runnable runnable) {
        try {
            outstandingTasks.incrementAndGet();
            databaseWorkers.submit(() -> {
                activeTasks.incrementAndGet();
                try {
                    runnable.run();
                } catch (Exception e) {
                    failedTasks.incrementAndGet();
                    throw e;
                } finally {
                    completedTasks.incrementAndGet();
                    activeTasks.decrementAndGet();
                    outstandingTasks.decrementAndGet();
                }
            });

            submittedTasks.incrementAndGet();

        } catch (RejectedExecutionException e) {
            outstandingTasks.decrementAndGet();
            rejectedTasks.incrementAndGet();
            throw e;
        }
    }

    public void setMessageDispatcher(MessageDispatcher messageDispatcher) {
        this.messageDispatcher = messageDispatcher;
    }

    @Override
    public synchronized HealthReport checkHealth() {

        Map<String, Object> metrics = new LinkedHashMap<>();

        metrics.put("isQuiescing", isQuiescing);
        metrics.put("dataSourceInitialized", dataSource != null);
        metrics.put("executorInitialized", databaseWorkers != null);

        metrics.put("submittedTasks", submittedTasks.get());
        metrics.put("completedTasks", completedTasks.get());
        metrics.put("failedTasks", failedTasks.get());
        metrics.put("rejectedTasks", rejectedTasks.get());
        metrics.put("rejectedDuringQuiesce", rejectedDuringQuiesce.get());

        metrics.put("activeTasks", activeTasks.get());
        metrics.put("outstandingTasks", outstandingTasks.get());

        metrics.put("connectionAcquisitionFailures",
                connectionAcquisitionFailures.get());

        if (databaseWorkers != null) {
            metrics.put("executorShutdown", databaseWorkers.isShutdown());
            metrics.put("executorTerminated", databaseWorkers.isTerminated());
        }

        HealthStatus status;
        String message;

        if (databaseWorkers == null || dataSource == null) {
            status = HealthStatus.UNREACHABLE;
            message = "Database service has not been initialized.";
        } else if (databaseWorkers.isTerminated() || databaseWorkers.isShutdown()) {
            status = HealthStatus.UNHEALTHY;
            message = "Database worker executor is no longer operational.";
        } else {
            boolean databaseConnectionAvailable = checkDatabaseConnection();

            metrics.put("databaseConnectionAvailable",
                    databaseConnectionAvailable);

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

    private boolean checkDatabaseConnection() {
        try (Connection connection = dataSource.getConnection()) {
            return connection.isValid(2);
        } catch (SQLException e) {
            return false;
        }
    }
}
