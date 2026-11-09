package org.prats.services.containerService;

import org.prats.container.ContainerInitializer;
import org.prats.definitions.ContainerType;
import org.prats.definitions.HealthCheckable;
import org.prats.definitions.HealthReport;
import org.prats.definitions.HealthStatus;
import org.prats.loggers.ShellLogger;
import org.prats.container.ContainerProcess;
import org.prats.services.HealthReportAnalyzer;
import org.prats.services.Service;
import org.prats.registries.ServiceRegistry;
import org.prats.services.ServiceFailureReporter;
import org.prats.services.healthCheckService.HealthChecker;
import org.prats.threading.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;


// the container service is a lifecycle manager of all the services that run inside the container(jvm),
// it can decide when to start drain or quiesce a service. no other service inside the jvm should have this capability
// if the container service detects that the jvm is not healthy, it can do its own re-initialization
// however if that fails, it will ask the platform to kill itself and restart again
// if that fails as well, the platform should be able to detect that container service unresponsive and
// initiate its restart, only an external jvm which runs the platform jvm can kill this service
public class ContainerService implements Service, ServiceFailureReporter, HealthCheckable {

    private final ContainerType containerType;
    private final ServiceRegistry serviceRegistry;
    private final ExecutorService containerWorkers;
    private final HealthChecker healthChecker;
    private final HealthReportAnalyzer healthReportAnalyzer;
    private final List<ContainerProcess> containerProcesses;
    private final Map<Service, List<HealthReport>> recentFailureReports;

    private final int tcpPort, tcpBacklog, coreThreads, maxThreads;
    private final long idleTimeout;
    private final String serviceName;
    private final String databaseConfigFile;
    private final Map<Service, Integer> failedServices;

    private volatile ScheduledExecutorService scheduledThread;

    public ContainerService(
            int coreThreads,
            int maxThreads,
            long idleTimeout,
            int tcpPort,
            int tcpBacklog,
            String serviceName,
            String databaseConfigFile,
            ContainerType containerType,
            ServiceRegistry serviceRegistry,
            HealthChecker healthChecker,
            HealthReportAnalyzer healthReportAnalyzer,
            List<ContainerProcess> containerProcesses,
            ContainerInitializer containerInitializer
            // storing a reference here as the container service may decide to do a full restart of all the services using itself if
            // it decided that is appropriate, the restart of services not the entire jvm
    ) {

        this.coreThreads            = coreThreads;
        this.maxThreads             = maxThreads;
        this.idleTimeout            = idleTimeout;

        this.tcpPort                = tcpPort;
        this.tcpBacklog             = tcpBacklog;
        this.serviceName            = serviceName;
        this.databaseConfigFile     = databaseConfigFile;

        this.containerType          = containerType;
        this.serviceRegistry        = serviceRegistry;
        this.healthChecker          = healthChecker;
        this.healthReportAnalyzer   = healthReportAnalyzer;

        this.containerProcesses     = containerProcesses;

        this.failedServices         = new ConcurrentHashMap<>();
        this.recentFailureReports   = new ConcurrentHashMap<>();

        this.containerWorkers       = new ThreadPool().initializeAndGetThreadPool(
                coreThreads, maxThreads, idleTimeout, true
        );
    }

    // through this function  health services can report to the container service about the services that are not running
    public void reportServiceFailure(Service service) {
        failedServices.merge(service, 1, Integer::sum);
    }

    // submits task to platform workers
    private void submitToThreadPool(Runnable runnable) {
        containerWorkers.submit(
                runnable
        );
    }


    @Override
    public void start() {
        for (Service service: serviceRegistry.getAllServices()) {
            try {
                service.start();
            } catch (Exception exception) {
                // logFailedService(service);
                // the service didn't start, not good... had put exception handling here as one service going down
                // should not make other services not start as well. we expect that healthService will log this to
                // container service and container service will do a recovery of the failed service later
                // however i doubt how much that recovery would be useful
            }
        }

    }

    // the shutdown is in the reverse order of startup
    @Override
    public void shutdown() {
        for (Service service: serviceRegistry.getAllServices().reversed()) {
            service.shutdown();
        }
    }

    // orders all the services to stop accepting new work
    @Override
    public void quiesce() {
        // draining the services in reverse order
        for (Service service: serviceRegistry.getAllServices().reversed()) {
            service.quiesce();
        }
    }

    // gives every service some time to drain themselves
    @Override
    public void drain(long totalTimeout) {
        // draining the services in reverse order
        long individualServiceTimeout = totalTimeout / serviceRegistry.getSize();
        for (Service service: serviceRegistry.getAllServices().reversed()) {
            service.drain(individualServiceTimeout);
        }
    }

    // restarts the service within the given timeout
    private void restartService(Service service, long timeout) {
        service.quiesce();      // stop accepting work
        service.drain(timeout); // drain accepted work
        service.shutdown();     // force shutdown
        service.start();        // start fresh
    }

    //
    @Override
    public void reportServiceFailure(
            Service service,
            List<HealthReport> recentHealthReports
    ) {
        reportServiceFailure(service);

        recentFailureReports.put(
                service,
                List.copyOf(recentHealthReports)
        );
    }

    // the health services can report to the container that the failed service is now back up so it can stop monitoring
    @Override
    public void reportServiceRecovered(Service service) {
        failedServices.remove(service);
        recentFailureReports.remove(service);
    }

    private HealthStatus checkHealthOfHealthChecker() {

        if (!(healthChecker instanceof Service service)) {
            return HealthStatus.UNKNOWN;
        }

        List<HealthReport> healthReports =
                healthChecker.getRecentHealthReports(service);

        if (healthReports == null || healthReports.isEmpty())
            return HealthStatus.UNREACHABLE;

        return healthReportAnalyzer.analyze(healthReports);
    }

    private void triggerHealthCheckerHealthCheck() {
        CompletableFuture
                .supplyAsync(this::checkHealthOfHealthChecker, containerWorkers)
                .thenAcceptAsync(this::evaluateHealthCheckerResponse, containerWorkers)
                .exceptionally(exception -> {
                    ShellLogger.log("Health checker health evaluation failed: "
                            + exception.getMessage());
                    return null;
                });
    }

    private void evaluateHealthCheckerResponse(HealthStatus healthStatus) {
        Service service = (Service) healthChecker;
        reportServiceFailure(service);

        if (failedServices.get(service) > 3)
            restartService(service, 1_000);
    }

    @Override
    public HealthReport checkHealth() {
        return null;
    }
}
