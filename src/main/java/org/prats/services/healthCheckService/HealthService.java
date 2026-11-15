
package org.prats.services.healthCheckService;

import org.prats.definitions.classes.ProcessContext;
import org.prats.definitions.interfaces.*;
import org.prats.definitions.records.ConsolidatedHealthReport;
import org.prats.definitions.records.HealthReport;
import org.prats.definitions.enums.HealthStatus;
import org.prats.loggers.EngineLogger;
import org.prats.utilities.CircularBuffer;
import org.prats.utilities.HealthReportAnalyzer;

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

    private static final EngineLogger log =
            EngineLogger.getLogger(HealthService.class);

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

    private volatile ServiceStatusReporter serviceStatusReporter;

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

        log.debug(() ->
                "[HealthService][constructor] Initializing HealthService"
                        + " | healthCheckRetention=" + healthCheckRetention
                        + " | loggingThreshold=" + loggingThreshold
        );

        this.isQuiescing            = true;
        this.serviceName            = this.getClass().getSimpleName();
        this.serviceRegistry        = serviceRegistry;
        this.healthCheckRetention   = healthCheckRetention;
        this.loggingThreshold       = loggingThreshold;
        this.healthReportAnalyzer   = new HealthReportAnalyzer();

        log.info(() ->
                "[HealthService][constructor] HealthService initialized"
                        + " | serviceName=" + serviceName
                        + " | healthCheckRetention=" + healthCheckRetention
                        + " | loggingThreshold=" + loggingThreshold
        );
    }

    public void setServiceFailureReporter(ServiceStatusReporter serviceStatusReporter) {

        log.debug("[HealthService][setServiceFailureReporter] Configuring service failure reporter");

        this.serviceStatusReporter = serviceStatusReporter;

        log.info(() ->
                "[HealthService][setServiceFailureReporter] Service failure reporter configured"
                        + " | configured=" + (serviceStatusReporter != null)
        );
    }

    public void analyzeAndStoreHealthReport(
            Service service,
            HealthReport healthReport
    ) {

        log.debug(() ->
                "[HealthService][analyzeAndStoreHealthReport] Processing health report"
                        + " | service="
                        + (service == null ? "null" : service.getClass().getSimpleName())
                        + " | reportStatus="
                        + (healthReport == null ? "null" : healthReport.status())
        );

        Objects.requireNonNull(service, "Service cannot be null");
        Objects.requireNonNull(healthReport, "Health report cannot be null");

        // Resolve the unique registry identity of this service instance.
        String serviceId = serviceRegistry.getServiceId(service);

        log.debug(() ->
                "[HealthService][analyzeAndStoreHealthReport] Service identity resolved"
                        + " | serviceId=" + serviceId
        );

        // Normalize the report name so that reports carry the unique identity.
        HealthReport identifiedReport = new HealthReport(
                serviceId,
                healthReport.status(),
                healthReport.message(),
                healthReport.timestamp(),
                healthReport.metrics()
        );

        Map<String, HealthReport> serviceHealth = new LinkedHashMap<>();

        serviceHealth.put(
                serviceId + "_" + identifiedReport.timestamp(),
                identifiedReport
        );

        log.debug(() ->
                "[HealthService][analyzeAndStoreHealthReport] Adding report to circular buffer"
                        + " | serviceId=" + serviceId
                        + " | status=" + identifiedReport.status()
                        + " | timestamp=" + identifiedReport.timestamp()
        );

        healthReports.add(serviceHealth);

        Deque<HealthReport> serviceReports =
                recentReportsByService.computeIfAbsent(
                        serviceId,
                        key -> new ArrayDeque<>()
                );

        List<HealthReport> recentHealthReports;

        synchronized (serviceReports) {

            serviceReports.addLast(identifiedReport);

            while (serviceReports.size() > healthCheckRetention) {
                serviceReports.removeFirst();
            }

            recentHealthReports = List.copyOf(serviceReports);
        }

        log.debug(() ->
                "[HealthService][analyzeAndStoreHealthReport] Recent health report history updated"
                        + " | serviceId=" + serviceId
                        + " | retainedReports=" + recentHealthReports.size()
                        + " | retentionLimit=" + healthCheckRetention
        );

        if (service == this || serviceStatusReporter == null) {

            log.debug(() ->
                    "[HealthService][analyzeAndStoreHealthReport] Skipping failure reporter notification"
                            + " | serviceIsHealthService=" + (service == this)
                            + " | reporterConfigured=" + (serviceStatusReporter != null)
            );

            return;
        }

        try {

            log.debug(() ->
                    "[HealthService][analyzeAndStoreHealthReport] Analyzing recent service health reports"
                            + " | serviceId=" + serviceId
                            + " | reportCount=" + recentHealthReports.size()
            );

            HealthStatus analyzedStatus =
                    healthReportAnalyzer.analyze(recentHealthReports);

            log.debug(() ->
                    "[HealthService][analyzeAndStoreHealthReport] Health analysis completed"
                            + " | serviceId=" + serviceId
                            + " | analyzedStatus=" + analyzedStatus
            );

            if (analyzedStatus == HealthStatus.HEALTHY) {

                log.info(() ->
                        "[HealthService][analyzeAndStoreHealthReport] Service reported recovered"
                                + " | serviceId=" + serviceId
                );

                serviceStatusReporter.reportServiceRecovered(service);

            } else {

                log.warn(() ->
                        "[HealthService][analyzeAndStoreHealthReport] Service health requires attention"
                                + " | serviceId=" + serviceId
                                + " | analyzedStatus=" + analyzedStatus
                                + " | reportCount=" + recentHealthReports.size()
                );

                serviceStatusReporter.reportServiceFailure(
                        service,
                        recentHealthReports
                );
            }

        } catch (Exception exception) {

            log.error(
                    "[HealthService][analyzeAndStoreHealthReport] Failure reporter exception"
                            + " | serviceId=" + serviceId,
                    exception
            );
        }
    }


    private HealthReport createServiceUnreachableHealthReport(
            Service service
    ) {

        log.warn(() ->
                "[HealthService][createServiceUnreachableHealthReport] Creating unreachable health report"
                        + " | service="
                        + (service == null ? "null" : service.getClass().getSimpleName())
        );

        return new HealthReport(
                serviceRegistry.getServiceId(service),
                HealthStatus.UNREACHABLE,
                serviceRegistry.getServiceId(service) + " is unreachable",
                Instant.now(),
                new HashMap<>()
        );
    }

    @Override
    public void performHealthChecks() {

        log.debug("[HealthService][performHealthChecks] Health check cycle requested");

        synchronized (healthCheckLifecycleLock) {

            if (isQuiescing) {
                return;
            }

            if (!healthCheckInProgress.compareAndSet(false, true)) {
                return;
            }
        }

        long cycleNumber = healthCheckCycles.get() + 1;

        log.debug(() ->
                "[HealthService][performHealthChecks] Health check cycle started"
                        + " | cycleNumber=" + cycleNumber
        );

        lastCycleFailed = false;
        lastCycleFailureMessage = null;

        int checkedServices = 0;
        int unreachableServices = 0;

        try {

            for (Service service : serviceRegistry.getAllServices()) {

                if (service instanceof HealthCheckable healthCheckable) {

                    String checkedServiceName = service.getClass().getSimpleName();

                    log.debug(() ->
                            "[HealthService][performHealthChecks] Checking service health"
                                    + " | service=" + checkedServiceName
                    );

                    HealthReport healthReport;

                    try {

                        healthReport = healthCheckable.checkHealth();

                        if (healthReport == null) {

                            log.warn(() ->
                                    "[HealthService][performHealthChecks] Service returned a null health report"
                                            + " | service=" + checkedServiceName
                            );

                            healthReport = createServiceUnreachableHealthReport(service);
                        } else {

                            final HealthReport receivedHealthReport = healthReport;

                            log.debug(() ->
                                    "[HealthService][performHealthChecks] Health report received"
                                            + " | service=" + checkedServiceName
                                            + " | status=" + receivedHealthReport.status()
                            );
                        }

                    } catch (Exception exception) {
                        // this is bad, could not get response from service...

                        log.error(
                                "[HealthService][performHealthChecks] Exception while checking service health"
                                        + " | service=" + checkedServiceName,
                                exception
                        );

                        healthReport = createServiceUnreachableHealthReport(service);
                    }

                    checkedServices++;

                    if (healthReport.status() == HealthStatus.UNREACHABLE) {

                        unreachableServices++;

                        log.warn(() ->
                                "[HealthService][performHealthChecks] Service is unreachable"
                                        + " | service=" + checkedServiceName
                        );
                    }

                    analyzeAndStoreHealthReport(service, healthReport);
                }
            }

        } catch (Exception exception) {

            lastCycleFailed = true;
            lastCycleFailureMessage = exception.getMessage();
            failedHealthCheckCycles.incrementAndGet();

            log.error(
                    "[HealthService][performHealthChecks] Health check cycle failed"
                            + " | cycleNumber=" + cycleNumber
                            + " | checkedServices=" + checkedServices
                            + " | unreachableServices=" + unreachableServices,
                    exception
            );

        } finally {

            servicesCheckedLastCycle.set(checkedServices);
            unreachableServicesLastCycle.set(unreachableServices);

            healthCheckCycles.incrementAndGet();
            lastHealthCheckCompletedAt = Instant.now();

            healthCheckInProgress.set(false);

            log.debug(() ->
                    "[HealthService][performHealthChecks] Health check cycle completed"
                            + " | cycleNumber=" + healthCheckCycles.get()
                            + " | checkedServices=" + servicesCheckedLastCycle.get()
                            + " | unreachableServices=" + unreachableServicesLastCycle.get()
                            + " | cycleFailed=" + lastCycleFailed
                            + " | failedCycles=" + failedHealthCheckCycles.get()
            );
        }
    }

    @Override
    public void start() {

        log.info("[HealthService][start] HealthService startup requested");

        synchronized (healthCheckLifecycleLock) {

            if (scheduledThread != null &&
                    !scheduledThread.isShutdown() &&
                    !isQuiescing) {

                log.warn("[HealthService][start] HealthService is already running; ignoring duplicate start request");

                return;
            }

            log.debug("[HealthService][start] Initializing health report circular buffer");

            healthReports = new CircularBuffer<>(healthCheckRetention, loggingThreshold);

            if (scheduledThread == null || scheduledThread.isTerminated()) {

                log.debug("[HealthService][start] Creating health check scheduler");

                scheduledThread = Executors.newSingleThreadScheduledExecutor();

                log.info("[HealthService][start] Health check scheduler initialized");

            } else if (scheduledThread.isShutdown()) {

                log.error("[HealthService][start] Previous scheduler has not terminated; startup cannot continue");

                throw new IllegalStateException("[Health Service] Previous scheduler has not terminated. Abandoning Start.");
            }

            startedAt                   = Instant.now();
            lastHealthCheckCompletedAt  = null;
            lastCycleFailed             = false;
            lastCycleFailureMessage     = null;

            isQuiescing                 = false;

            log.debug(() ->
                    "[HealthService][start] Scheduling periodic health checks"
                            + " | intervalSeconds=" + HEALTH_CHECK_INTERVAL_SECONDS
                            + " | initialDelaySeconds=0"
            );

            scheduledHealthChecks = scheduledThread.scheduleWithFixedDelay(
                    () -> {

                        log.debug("[HealthService][start] Scheduled health check execution triggered");

                        try {

                            performHealthChecks();

                        } catch (Exception e) {
                            // this exception catching is imp as if this thread dies, the health check service would die and won't be able to check services now...

                            log.error(
                                    "[HealthService][start] Unhandled exception during scheduled health check execution",
                                    e
                            );
                        }
                    },
                    0,
                    HEALTH_CHECK_INTERVAL_SECONDS,
                    TimeUnit.SECONDS
            );

            log.info("[HealthService][start] HealthService started successfully");
        }
    }

    @Override
    public void shutdown() {

        log.info("[HealthService][shutdown] HealthService shutdown requested");

        quiesce();

        ScheduledExecutorService scheduler = scheduledThread;

        if (scheduler == null) {

            log.debug("[HealthService][shutdown] No scheduler is currently initialized");

            return;
        }

        log.debug("[HealthService][shutdown] Initiating graceful scheduler shutdown");

        scheduler.shutdown();

        try {

            log.debug("[HealthService][shutdown] Waiting for scheduler termination");

            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {

                log.warn("[HealthService][shutdown] Graceful scheduler shutdown timed out; forcing shutdown");

                scheduler.shutdownNow();

                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {

                    log.error("[HealthService][shutdown] HealthService scheduler failed to terminate.");

                    return;
                }
            }

            if (scheduledThread == scheduler) {

                scheduledThread = null;

                log.debug("[HealthService][shutdown] Scheduler reference cleared");
            }

            log.info("[HealthService][shutdown] HealthService shutdown completed");

        } catch (InterruptedException e) {

            log.error(
                    "[HealthService][shutdown] Interrupted while waiting for scheduler termination",
                    e
            );

            scheduler.shutdownNow();

            Thread.currentThread().interrupt();

            if (scheduler.isTerminated() && scheduledThread == scheduler) {

                scheduledThread = null;

                log.debug("[HealthService][shutdown] Terminated scheduler reference cleared after interruption");
            }
        }
    }

    @Override
    public void quiesce() {

        log.info("[HealthService][quiesce] HealthService quiescence requested");

        boolean alreadyQuiescing;
        synchronized (healthCheckLifecycleLock) {
            alreadyQuiescing = isQuiescing;
            if (!alreadyQuiescing) {
                isQuiescing = true;
                if (scheduledHealthChecks != null) scheduledHealthChecks.cancel(false);
            }
        }

        if (alreadyQuiescing) {
            log.debug("[HealthService][quiesce] HealthService is already quiescing");
            return;
        }

        log.debug("[HealthService][quiesce] Health check admission disabled");

        log.info("[HealthService][quiesce] HealthService is now quiescing");
    }

    @Override
    public void drain(long timeout) {

        log.debug(() ->
                "[HealthService][drain] Waiting for active health check to complete"
                        + " | timeoutMillis=" + timeout
                        + " | healthCheckInProgress=" + healthCheckInProgress.get()
        );

        long deadline = System.nanoTime() +
                TimeUnit.MILLISECONDS.toNanos(Math.max(0, timeout));

        while (healthCheckInProgress.get()) {

            if (Thread.currentThread().isInterrupted()) {

                log.warn("[HealthService][drain] Drain interrupted while waiting for health check completion");

                return;
            }

            long remainingTime = deadline - System.nanoTime();

            if (remainingTime <= 0) {

                log.warn(() ->
                        "[HealthService][drain] Drain timeout reached while health check was still in progress"
                                + " | timeoutMillis=" + timeout
                );

                return;
            }

            LockSupport.parkNanos(
                    Math.min(
                            TimeUnit.MILLISECONDS.toNanos(50),
                            remainingTime
                    )
            );
        }

        log.debug("[HealthService][drain] HealthService drain completed");
    }

    @Override
    public void cleanup() {

        log.debug("[HealthService][cleanup] Cleanup invoked");
    }

    @Override
    public List<HealthReport> getRecentHealthReports(Service service) {

        log.debug(() ->
                "[HealthService][getRecentHealthReports] Retrieving recent health reports"
                        + " | service="
                        + (service == null ? "null" : service.getClass().getSimpleName())
        );

        String serviceId = serviceRegistry.getServiceId(service);

        Deque<HealthReport> serviceReports =
                recentReportsByService.get(serviceId);

        if (serviceReports == null) {

            log.debug(() ->
                    "[HealthService][getRecentHealthReports] No health reports found"
                            + " | serviceId=" + serviceId
            );

            return List.of();
        }

        List<HealthReport> result;
        synchronized (serviceReports) {
            result = List.copyOf(serviceReports);
        }

        log.debug(() ->
                "[HealthService][getRecentHealthReports] Recent health reports retrieved"
                        + " | serviceId=" + serviceId
                        + " | reportCount=" + result.size()
        );
        return result;
    }

    @Override
    public List<HealthReport> getRecentHealthReports() {

        log.debug("[HealthService][getRecentHealthReports] Retrieving health reports for all services");

        List<HealthReport> reports = new ArrayList<>();

        for (Service service : serviceRegistry.getAllServices()) {

            if (service instanceof HealthCheckable) {

                reports.addAll(getRecentHealthReports(service));
            }
        }

        log.debug(() ->
                "[HealthService][getRecentHealthReports] Health reports retrieved for all services"
                        + " | reportCount=" + reports.size()
        );

        return reports;
    }

    @Override
    public HealthReport checkHealth() {

        log.debug("[HealthService][checkHealth] HealthService health check requested");

        Map<String, Object> metrics = new LinkedHashMap<>();

        metrics.put("healthCheckCycles", healthCheckCycles.get());
        metrics.put("failedHealthCheckCycles", failedHealthCheckCycles.get());
        metrics.put("servicesCheckedLastCycle", servicesCheckedLastCycle.get());
        metrics.put("unreachableServicesLastCycle", unreachableServicesLastCycle.get());

        metrics.put("healthCheckInProgress", healthCheckInProgress.get());
        metrics.put("failureReporterConfigured", serviceStatusReporter != null);

        metrics.put("startedAt", startedAt);
        metrics.put("healthServiceUptimeSeconds", startedAt == null
                ? 0
                : Math.max(0, Duration.between(startedAt, Instant.now()).getSeconds()));
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

            log.warn("[HealthService][checkHealth] Health service has not been started");

        } else if (scheduledThread.isShutdown() || scheduledThread.isTerminated()) {

            status = HealthStatus.UNHEALTHY;
            message = "Health monitoring scheduler is no longer operational.";

            log.error("[HealthService][checkHealth] Health monitoring scheduler is no longer operational");

        } else if (lastHealthCheckCompletedAt == null) {

            if (Duration.between(startedAt, Instant.now()).getSeconds()
                    > HEALTH_CHECK_STALE_SECONDS) {

                status = HealthStatus.UNREACHABLE;

                message = "Health service has not completed a health check within the expected period.";

                log.warn("[HealthService][checkHealth] First health check has not completed within the expected period");

            } else {

                status = HealthStatus.DEGRADED;

                message = "Waiting for the first health check to complete.";

                log.debug("[HealthService][checkHealth] Waiting for first health check to complete");
            }

        } else if (Duration.between(
                lastHealthCheckCompletedAt,
                Instant.now()
        ).getSeconds() > HEALTH_CHECK_STALE_SECONDS) {

            status = HealthStatus.UNREACHABLE;

            message = "Health checks are no longer being completed within the expected period.";

            log.warn("[HealthService][checkHealth] Health check completion is stale");

        } else if (lastCycleFailed) {

            status = HealthStatus.DEGRADED;

            message = "The latest health check cycle failed.";

            log.warn(() ->
                    "[HealthService][checkHealth] Latest health check cycle failed"
                            + " | failureMessage=" + lastCycleFailureMessage
            );

        } else {

            status = HealthStatus.HEALTHY;

            message = "Health monitoring is operating normally.";

            log.debug("[HealthService][checkHealth] HealthService is healthy");
        }

        log.debug(() ->
                "[HealthService][checkHealth] HealthService health evaluation completed"
                        + " | status=" + status
                        + " | healthServiceUptimeSeconds="
                        + metrics.get("healthServiceUptimeSeconds")
                        + " | healthCheckCycles=" + healthCheckCycles.get()
                        + " | failedHealthCheckCycles=" + failedHealthCheckCycles.get()
                        + " | servicesCheckedLastCycle=" + servicesCheckedLastCycle.get()
                        + " | unreachableServicesLastCycle=" + unreachableServicesLastCycle.get()
        );

        return new HealthReport(
                serviceName,
                status,
                message,
                Instant.now(),
                metrics
        );
    }

    @Override
    public ConsolidatedHealthReport getConsolidatedHealthReport() {

        log.debug("[HealthService][getConsolidatedHealthReport] Consolidated health report generation started");

        Map<String, HealthReport> consolidatedServices = new LinkedHashMap<>();


        for (Service service : serviceRegistry.getAllServices()) {

            if (!(service instanceof HealthCheckable)) {
                continue;
            }

            String serviceId = serviceRegistry.getServiceId(service);

            log.debug(() ->
                    "[HealthService][getConsolidatedHealthReport] Processing service health history"
                            + " | serviceId=" + serviceId
            );

            List<HealthReport> recentReports =
                    getRecentHealthReports(service);

            HealthReport consolidatedReport;

            if (recentReports.isEmpty()) {

                log.warn(() ->
                        "[HealthService][getConsolidatedHealthReport] No health report has been recorded for service"
                                + " | serviceId=" + serviceId
                );

                consolidatedReport = new HealthReport(
                        serviceId,
                        HealthStatus.UNREACHABLE,
                        "No health report has been recorded yet.",
                        Instant.now(),
                        Map.of()
                );

            } else {


                HealthReport latestReport =
                        recentReports.get(recentReports.size() - 1);


                HealthStatus analyzedStatus =
                        healthReportAnalyzer.analyze(recentReports);

                String message = latestReport.message();

                log.debug(() ->
                        "[HealthService][getConsolidatedHealthReport] Service health history analyzed"
                                + " | serviceId=" + serviceId
                                + " | analyzedStatus=" + analyzedStatus
                                + " | latestStatus=" + latestReport.status()
                                + " | reportCount=" + recentReports.size()
                );

                if (analyzedStatus != latestReport.status()) {

                    message = "Recent health analysis: " + analyzedStatus
                            + ". Latest check: " + latestReport.message();
                }

                consolidatedReport = new HealthReport(
                        serviceId,
                        analyzedStatus,
                        message,
                        latestReport.timestamp(),
                        latestReport.metrics()
                );
            }

            consolidatedServices.put(serviceId, consolidatedReport);
        }


        List<HealthReport> serviceReports =
                new ArrayList<>(consolidatedServices.values());

        HealthStatus overallStatus =
                serviceReports.isEmpty()
                        ? HealthStatus.DEGRADED
                        : healthReportAnalyzer.aggregateServices(serviceReports);


        long healthy = serviceReports.stream()
                .filter(r -> r.status() == HealthStatus.HEALTHY)
                .count();

        long degraded = serviceReports.stream()
                .filter(r -> r.status() == HealthStatus.DEGRADED)
                .count();

        long unhealthy = serviceReports.stream()
                .filter(r -> r.status() == HealthStatus.UNHEALTHY)
                .count();

        long unreachable = serviceReports.stream()
                .filter(r -> r.status() == HealthStatus.UNREACHABLE)
                .count();


        String overallMessage = String.format(
                "Container health: %s. Total services: %d. "
                        + "Healthy: %d, Degraded: %d, "
                        + "Unhealthy: %d, Unreachable: %d.",
                overallStatus,
                serviceReports.size(),
                healthy,
                degraded,
                unhealthy,
                unreachable
        );

        log.info(() ->
                "[HealthService][getConsolidatedHealthReport] Consolidated health report generated"
                        + " | overallStatus=" + overallStatus
                        + " | totalServices=" + serviceReports.size()
                        + " | healthy=" + healthy
                        + " | degraded=" + degraded
                        + " | unhealthy=" + unhealthy
                        + " | unreachable=" + unreachable
        );

        return new ConsolidatedHealthReport(
                ProcessContext.getProcessName(),
                overallStatus,
                overallMessage,
                Instant.now(),
                consolidatedServices,
                org.prats.utilities.ContainerRuntime.getStartedAt()
        );
    }
}
