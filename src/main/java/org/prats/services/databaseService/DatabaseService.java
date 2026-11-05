package org.prats.services.databaseService;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.prats.database.h2Database.H2DatabaseInitializer;
import org.prats.definitions.HealthCheckable;
import org.prats.definitions.HealthReport;
import org.prats.definitions.HealthStatus;
import org.prats.definitions.MessageEntity;
import org.prats.inputHandler.handlers.DatabaseMessageHandler;
import org.prats.loggers.ShellLogger;
import org.prats.services.Service;
import org.prats.services.messageService.MessageService;
import org.prats.threading.ThreadPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class DatabaseService implements Service, HealthCheckable {

    private HikariDataSource dataSource;
    private final H2DatabaseInitializer databaseInitializer;

    private final AtomicInteger activeTasks;
    private final LongAdder submittedTasks;
    private final LongAdder completedTasks;
    private final LongAdder rejectedTasks;
    private final LongAdder failedTasks;
    private final Object admissionLock;

    private MessageService messageService;
    private final ExecutorService threadPool;
    private final DatabaseMessageHandler databaseMessageHandler;
    private final String serviceName;

    private volatile boolean acceptWork;

    public DatabaseService(
            int coreThreads,
            int maxThreads,
            int idleTimeout,
            String databaseConfigFile
    ) {
        this.acceptWork             = false;
        this.serviceName            = this.getClass().getSimpleName();
        this.databaseInitializer    = new H2DatabaseInitializer(databaseConfigFile);
        this.databaseMessageHandler = new DatabaseMessageHandler(this);
        this.activeTasks            = new AtomicInteger();
        this.submittedTasks         = new LongAdder();
        this.completedTasks         = new LongAdder();
        this.rejectedTasks          = new LongAdder();
        this.failedTasks            = new LongAdder();
        this.admissionLock          = new Object();
        threadPool                  = new ThreadPool().initializeAndGetThreadPool(
                coreThreads,
                maxThreads,
                idleTimeout,
                false
        );
    }

    @Override
    public Service startService() {
        databaseInitializer.initializeH2Database();
        dataSource = databaseInitializer.getH2DBConnectionPool();
        acceptWork = true;
        return this;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    // calling thread (message service) can submit the call to db thread and return to its pool
    public void submit(MessageEntity messageEntity) {
        synchronized (admissionLock) {
            if (acceptWork)
                submitToThreadPool(() -> {
                    databaseMessageHandler.handleMessage(messageEntity);
                });
            else {
                // TODO: this is important, a message needs to be delivered back to the caller that the db service is not accepting work,
                // otherwise, the calling service would not know what happened with their request
            }
        }
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public void setMessageService(MessageService messageService) {
        this.messageService = messageService;
    }

    public void sendMessage(MessageEntity messageEntity) {
        messageService.submit(
            messageEntity
        );
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

    @Override
    public void shutdownService() {

        synchronized (admissionLock) {
            acceptWork = false;
        }

        threadPool.shutdown();

        try {
            if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();

                if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            "Database executor failed to terminate"
                    );
                }
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted while shutting down DatabaseService", e
            );
        }

        if (dataSource != null) {
            dataSource.close();
        }
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

        boolean databaseAvailable = false;

        if (dataSource != null && !dataSource.isClosed()) {
            try (Connection connection = dataSource.getConnection()) {
                databaseAvailable = connection.isValid(2);
            } catch (SQLException e) {
                databaseAvailable = false;
            }
        }

        if (databaseAvailable) {
            HikariPoolMXBean pool = dataSource.getHikariPoolMXBean();

            metrics.put("activeConnections", pool.getActiveConnections());
            metrics.put("idleConnections", pool.getIdleConnections());
            metrics.put("totalConnections", pool.getTotalConnections());
            metrics.put("threadsAwaitingConnection",
                    pool.getThreadsAwaitingConnection());
        }

        boolean executorRunning = !threadPool.isShutdown()
                && !threadPool.isTerminated();

        metrics.put("databaseAvailable", databaseAvailable);
        metrics.put("executorRunning", executorRunning);

        HealthStatus status = databaseAvailable && executorRunning
                ? HealthStatus.HEALTHY
                : HealthStatus.UNHEALTHY;

        HealthReport healthReport = new HealthReport(
                getServiceName(),
                status,
                databaseAvailable && executorRunning
                        ? "Database service operational"
                        : "Database connection or threads unavailable",
                Instant.now(),
                metrics
        );

        ShellLogger.log("Report: " + healthReport + "\nHealth Check Ended for: " + this.getClass().getSimpleName());

        return healthReport;
    }
}
