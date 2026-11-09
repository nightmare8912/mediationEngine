
package org.prats.services.healthCheckService;

import org.prats.definitions.HealthCheckable;
import org.prats.definitions.HealthReport;
import org.prats.definitions.HealthStatus;
import org.prats.loggers.ShellLogger;
import org.prats.registries.ServiceRegistry;
import org.prats.services.Service;
import org.prats.services.ServiceFailureReporter;
import org.prats.utilities.CircularBuffer;
import org.prats.services.HealthReportAnalyzer;

import java.time.Instant;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class HealthService implements Service, HealthCheckable, HealthChecker {

    private static final long HEALTH_CHECK_INTERVAL_SECONDS = 10;
    private static final long HEALTH_CHECK_STALE_SECONDS = 30;

    private final String serviceName;
    private final ServiceRegistry serviceRegistry;
    private volatile ScheduledExecutorService scheduledThread;
    private volatile ScheduledFuture<?> scheduledHealthChecks;
    private final HealthReportAnalyzer healthReportAnalyzer;
    private final int healthCheckRetention;
    private final int loggingThreshold;

    private final Map<String, Deque<HealthReport>> recentReportsByService =
            new ConcurrentHashMap<>();

    private final AtomicBoolean healthCheckInProgress = new AtomicBoolean(false);
    private final AtomicLong healthCheckCycles = new AtomicLong();
    private final AtomicLong failedHealthCheckCycles = new AtomicLong();
    private final AtomicInteger servicesCheckedLastCycle = new AtomicInteger();
    private final AtomicInteger unreachableServicesLastCycle = new AtomicInteger();

    private final Object healthCheckLifecycleLock = new Object();

    private CircularBuffer<Map<String, HealthReport>> healthReports;

    private volatile ServiceFailureReporter serviceFailureReporter;

    private volatile Instant startedAt;
    private volatile Instant lastHealthCheckCompletedAt;
    private volatile boolean lastCycleFailed;
    private volatile boolean isQuiescing;
    private volatile String lastCycleFailureMessage;

    public HealthService(
            ServiceRegistry serviceRegistry,
            int healthCheckRetention,
            int loggingThreshold
    ) {
        this.isQuiescing            = true;
        this.serviceName            = this.getClass().getSimpleName();
        this.serviceRegistry        = serviceRegistry;
        this.healthCheckRetention   = healthCheckRetention;
        this.loggingThreshold       = loggingThreshold;
        this.healthReportAnalyzer   = new HealthReportAnalyzer();
    }

    public void setServiceFailureReporter(ServiceFailureReporter serviceFailureReporter) {
        this.serviceFailureReporter = serviceFailureReporter;
    }

    public void analyzeAndStoreHealthReport(Service service, HealthReport healthReport) {

        Map<String, HealthReport> serviceHealth = new LinkedHashMap<>();

        serviceHealth.put(service.getName() + "_" + healthReport.timestamp().toString(), healthReport);
        healthReports.add(serviceHealth);

        Deque<HealthReport> serviceReports = recentReportsByService.computeIfAbsent(
                service.getName(),
                key -> new ArrayDeque<>()
        );

        List<HealthReport> recentHealthReports;

        synchronized (serviceReports) {
            serviceReports.addLast(healthReport);

            while (serviceReports.size() > healthCheckRetention) {
                serviceReports.removeFirst();
            }
            recentHealthReports = List.copyOf(serviceReports);
        }

        if (service == this || serviceFailureReporter == null) {
            return;
        }

        try {

            HealthStatus analyzedStatus =
                    healthReportAnalyzer.analyze(recentHealthReports);

            if (analyzedStatus == HealthStatus.HEALTHY) {
                serviceFailureReporter.reportServiceRecovered(service);
            } else {
                serviceFailureReporter.reportServiceFailure(
                        service,
                        recentHealthReports
                );
            }

        } catch (Exception exception) {
            ShellLogger.log(
                    "[HealthService] [analyzeAndStoreHealthReport] Failure reporter exception: "
                            + exception.getMessage()
            );
        }
    }

    private HealthReport createServiceUnreachableHealthReport(Service service) {
        return new HealthReport(
                service.getName(),
                HealthStatus.UNREACHABLE,
                service.getName() + " is unreachable",
                Instant.now(),
                new HashMap<>()
        );
    }

    @Override
    public void performHealthChecks() {

        synchronized (healthCheckLifecycleLock) {
            if (isQuiescing ||
                    !healthCheckInProgress.compareAndSet(false, true)) {
                return;
            }
        }

        lastCycleFailed = false;
        lastCycleFailureMessage = null;

        int checkedServices = 0;
        int unreachableServices = 0;

        try {
            for (Service service : serviceRegistry.getAllServices()) {
                if (service instanceof HealthCheckable healthCheckable) {
                    HealthReport healthReport;
                    try {
                        healthReport = healthCheckable.checkHealth();

                        if (healthReport == null) {
                            healthReport = createServiceUnreachableHealthReport(service);
                        }

                    } catch (Exception exception) {
                        // this is bad, could not get response from service...
                        ShellLogger.log("[HealthService] [performHealthChecks] Exception: " + exception.getMessage());
                        healthReport = createServiceUnreachableHealthReport(service);
                    }
                    checkedServices++;
                    if (healthReport.status() == HealthStatus.UNREACHABLE) {
                        unreachableServices++;
                    }
                    analyzeAndStoreHealthReport(service, healthReport);
                }
            }

        } catch (Exception exception) {
            lastCycleFailed = true;
            lastCycleFailureMessage = exception.getMessage();
            failedHealthCheckCycles.incrementAndGet();
            ShellLogger.log(
                    "[HealthService] [performHealthChecks] Health check cycle failed: "
                            + exception.getMessage()
            );

        } finally {
            servicesCheckedLastCycle.set(checkedServices);
            unreachableServicesLastCycle.set(unreachableServices);

            healthCheckCycles.incrementAndGet();
            lastHealthCheckCompletedAt = Instant.now();

            healthCheckInProgress.set(false);
        }
    }

    @Override
    public void start() {
        synchronized (healthCheckLifecycleLock) {

            if (scheduledThread != null &&
                    !scheduledThread.isShutdown() &&
                    !isQuiescing) {
                return;
            }

            healthReports = new CircularBuffer<>(healthCheckRetention, loggingThreshold);

            if (scheduledThread == null || scheduledThread.isTerminated()) {
                scheduledThread = Executors.newSingleThreadScheduledExecutor();
            } else if (scheduledThread.isShutdown()) {
                throw new IllegalStateException("[Health Service] Previous scheduler has not terminated. Abandoning Start.");
            }

            startedAt                   = Instant.now();
            lastHealthCheckCompletedAt  = null;
            lastCycleFailed             = false;
            lastCycleFailureMessage     = null;

            isQuiescing                 = false;

            scheduledHealthChecks = scheduledThread.scheduleWithFixedDelay(
                    () -> {
                        try {
                            performHealthChecks();
                        } catch (Exception e) {
                            // this exception catching is imp as if this thread dies, the health check service would die and won't be able to check services now...
                            ShellLogger.log(e.getMessage());
                        }
                    },
                    0,
                    HEALTH_CHECK_INTERVAL_SECONDS,
                    TimeUnit.SECONDS
            );
        }
    }

    @Override
    public void shutdown() {
        quiesce();
        ScheduledExecutorService scheduler = scheduledThread;
        if (scheduler == null) {
            return;
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    ShellLogger.log("HealthService scheduler failed to terminate.");
                    return;
                }
            }
            if (scheduledThread == scheduler) {
                scheduledThread = null;
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
            if (scheduler.isTerminated() && scheduledThread == scheduler) {
                scheduledThread = null;
            }
        }
    }

    @Override
    public void quiesce() {

        synchronized (healthCheckLifecycleLock) {

            if (isQuiescing) {
                return;
            }

            isQuiescing = true;

            if (scheduledHealthChecks != null) {
                scheduledHealthChecks.cancel(false);
            }
        }
    }

    @Override
    public void drain(long timeout) {

        long deadline = System.nanoTime() +
                TimeUnit.MILLISECONDS.toNanos(Math.max(0, timeout));

        while (healthCheckInProgress.get()) {

            if (Thread.currentThread().isInterrupted()) {
                return;
            }

            long remainingTime = deadline - System.nanoTime();

            if (remainingTime <= 0) {
                return;
            }

            LockSupport.parkNanos(
                    Math.min(
                            TimeUnit.MILLISECONDS.toNanos(50),
                            remainingTime
                    )
            );
        }
    }

    @Override
    public List<HealthReport> getRecentHealthReports(Service service) {

        Deque<HealthReport> serviceReports =
                recentReportsByService.get(service.getName());

        if (serviceReports == null) {
            return List.of();
        }

        synchronized (serviceReports) {
            return List.copyOf(serviceReports);
        }
    }

    @Override
    public HealthReport checkHealth() {

        Map<String, Object> metrics = new LinkedHashMap<>();

        metrics.put("healthCheckCycles", healthCheckCycles.get());
        metrics.put("failedHealthCheckCycles", failedHealthCheckCycles.get());
        metrics.put("servicesCheckedLastCycle", servicesCheckedLastCycle.get());
        metrics.put("unreachableServicesLastCycle", unreachableServicesLastCycle.get());

        metrics.put("healthCheckInProgress", healthCheckInProgress.get());
        metrics.put("failureReporterConfigured", serviceFailureReporter != null);

        metrics.put("startedAt", startedAt);
        metrics.put("lastHealthCheckCompletedAt", lastHealthCheckCompletedAt);

        metrics.put("scheduledExecutorInitialized", scheduledThread != null);

        if (scheduledThread != null) {
            metrics.put("scheduledExecutorShutdown", scheduledThread.isShutdown());
            metrics.put("scheduledExecutorTerminated", scheduledThread.isTerminated());
        }

        if (lastCycleFailureMessage != null) {
            metrics.put("lastCycleFailureMessage", lastCycleFailureMessage);
        }

        HealthStatus status;
        String message;

        if (scheduledThread == null || startedAt == null) {
            status = HealthStatus.UNREACHABLE;
            message = "Health service has not been started.";

        } else if (scheduledThread.isShutdown() || scheduledThread.isTerminated()) {
            status = HealthStatus.UNHEALTHY;
            message = "Health monitoring scheduler is no longer operational.";

        } else if (lastHealthCheckCompletedAt == null) {

            if (Duration.between(startedAt, Instant.now()).getSeconds()
                    > HEALTH_CHECK_STALE_SECONDS) {
                status = HealthStatus.UNREACHABLE;
                message = "Health service has not completed a health check within the expected period.";
            } else {
                status = HealthStatus.DEGRADED;
                message = "Waiting for the first health check to complete.";
            }

        } else if (Duration.between(
                lastHealthCheckCompletedAt,
                Instant.now()
        ).getSeconds() > HEALTH_CHECK_STALE_SECONDS) {

            status = HealthStatus.UNREACHABLE;
            message = "Health checks are no longer being completed within the expected period.";

        } else if (lastCycleFailed) {

            status = HealthStatus.DEGRADED;
            message = "The latest health check cycle failed.";

        } else {
            status = HealthStatus.HEALTHY;
            message = "Health monitoring is operating normally.";
        }

        return new HealthReport(
                serviceName,
                status,
                message,
                Instant.now(),
                metrics
        );
    }
}
