package org.prats.services.healthCheckService;

import org.prats.definitions.HealthCheckable;
import org.prats.definitions.HealthReport;
import org.prats.definitions.MessageEntity;
import org.prats.loggers.ShellLogger;
import org.prats.services.Service;
import org.prats.services.ServiceRegistry;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HealthCheckService implements Service {

    private final String serviceName;
    private final ServiceRegistry serviceRegistry;
    private final ScheduledExecutorService scheduledThread;

    public HealthCheckService(
            ServiceRegistry serviceRegistry
    ) {
        this.serviceName        = this.getClass().getSimpleName();
        this.serviceRegistry    = serviceRegistry;
        scheduledThread         = Executors.newSingleThreadScheduledExecutor();
    }

    public void performHealthChecks() {
        for(Service service: serviceRegistry.getAllServices()) {
            if (service instanceof HealthCheckable healthCheckable) {
                try {
                    HealthReport healthReport = healthCheckable.checkHealth();
                } catch (Exception exe) {
                    // this is bad, could not get response from service...
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
