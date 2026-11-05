package org.prats.services.healthCheckService;

import org.prats.definitions.HealthCheckable;
import org.prats.definitions.HealthReport;
import org.prats.definitions.HealthStatus;
import org.prats.definitions.MessageEntity;
import org.prats.loggers.ShellLogger;
import org.prats.services.Service;
import org.prats.services.ServiceRegistry;
import org.prats.utilities.CircularBuffer;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HealthService implements Service {

    private final String serviceName;
    private final ServiceRegistry serviceRegistry;
    private final ScheduledExecutorService scheduledThread;
    private final CircularBuffer<Map<String, HealthReport>> healthReports;
    private final int healthCheckRetention;
    private final int loggingThreshold;

    public HealthService(
            ServiceRegistry serviceRegistry,
            int healthCheckRetention,
            int loggingThreshold
    ) {
        this.serviceName            = this.getClass().getSimpleName();
        this.serviceRegistry        = serviceRegistry;
        this.healthCheckRetention   = healthCheckRetention;
        this.loggingThreshold       = loggingThreshold;
        this.healthReports          = new CircularBuffer<>(healthCheckRetention, loggingThreshold);
        scheduledThread             = Executors.newSingleThreadScheduledExecutor();
    }

    public void analyzeAndStoreHealthReport(Service service, HealthReport healthReport) {
        Map<String, HealthReport> serviceHealth = new LinkedHashMap<>();

        serviceHealth.put(service.getServiceName() + " " + healthReport.timestamp().toString(), healthReport);
        healthReports.add(serviceHealth);

    }

    private HealthReport createServiceUnreachableHealthReport(Service service) {
        return new HealthReport(
                service.getServiceName(),
                HealthStatus.UNREACHABLE,
                service.getServiceName() + " is unreachable",
                Instant.now(),
                new HashMap<>()
        );
    }

    public void performHealthChecks() {
        for(Service service: serviceRegistry.getAllServices()) {
            if (service instanceof HealthCheckable healthCheckable) {
                try {
                    analyzeAndStoreHealthReport(service, healthCheckable.checkHealth());
                } catch (Exception exception) {
                    // this is bad, could not get response from service...
                    ShellLogger.log("[HealthService] [performHealthChecks] Exception: " + exception.getMessage());
                    analyzeAndStoreHealthReport(service, createServiceUnreachableHealthReport(service));
                }
            }
        }
    }

    @Override
    public Service startService() {
        scheduledThread.scheduleWithFixedDelay(
                () -> {
                    try {
                        performHealthChecks();
                    } catch (Exception e) {
                        // this exception catching is imp as if this thread dies, the health check service would die and won't be able to check services now...
                        ShellLogger.log(e.getMessage());
                    }
                },
                0,
                10,
                TimeUnit.SECONDS
        );
        return this;
    }

    @Override
    public void shutdownService() {
        scheduledThread.shutdown();

        try {
            if (!scheduledThread.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledThread.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledThread.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public void submit(MessageEntity messageEntity) {

    }
}
