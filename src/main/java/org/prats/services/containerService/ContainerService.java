package org.prats.services.containerService;

import org.prats.container.ContainerInitializer;
import org.prats.definitions.*;
import org.prats.handlers.CommandHandler;
import org.prats.loggers.ShellLogger;
import org.prats.container.ContainerProcess;
import org.prats.services.*;
import org.prats.registries.ServiceRegistry;
import org.prats.services.healthCheckService.HealthChecker;
import org.prats.threading.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;


// the container service is a lifecycle manager of all the services that run inside the container(jvm),
// it can decide when to start drain or quiesce a service. no other service inside the jvm should have this capability
// if the container service detects that the jvm is not healthy, it can do its own re-initialization
// however if that fails, it will ask the platform to kill itself and restart again
// if that fails as well, the platform should be able to detect that container service unresponsive and
// initiate its restart, only an external jvm which runs the platform jvm can kill this service
public class ContainerService implements Service, ServiceStatusReporter, CommandDispatcher, ServiceManager, HealthCheckable {

    private final ContainerType containerType;
    private final ServiceRegistry serviceRegistry;
    private final HealthChecker healthChecker;
    private final HealthReportAnalyzer healthReportAnalyzer;
    private final List<ContainerProcess> containerProcesses;
    private final Map<Service, List<HealthReport>> recentFailureReports;

    private final int tcpPort, tcpBacklog, coreThreads, maxThreads;
    private final long idleTimeout;
    private final String serviceName;
    private final String databaseConfigFile;
    private final Map<Service, Integer> failedServices;

    private ExecutorService containerWorkers;
    private CommandHandler commandHandler;
    private MessageDispatcher messageDispatcher;

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
    }

    public void setCommandHandler(CommandHandler commandHandler) {
        this.commandHandler = commandHandler;
    }

    public void setMessageDispatcher(MessageDispatcher messageDispatcher) {
        this.messageDispatcher = messageDispatcher;
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
        // start the container first
        containerWorkers       = new ThreadPool().initializeAndGetThreadPool(
                coreThreads, maxThreads, idleTimeout, true
        );

        // the services were inserted in an order in which we want them to start, and we iterate them and start in the same order
        for (Service service: serviceRegistry.getAllServices()) {
            try {
                service.start();
            } catch (Exception exception) {
                // logFailedService(service);
                // the service didn't start, not good... had put exception handling here as one service going down
                // should not make other services not start as well. we expect that healthService will log this to
                // container service and container service will do a recovery of the failed service later
                // however i doubt how much that recovery would be useful
                ShellLogger.log(
                        "[ContainerService] Failed to start "
                                + service.getClass().getSimpleName()
                                + ": "
                                + exception
                );

                exception.printStackTrace();
            }
        }

        scheduledThread = new ScheduledThreadPoolExecutor(1);

        // now we start check for health checker as it should have been started by now
        scheduledThread.scheduleAtFixedRate(
                this::triggerHealthCheckerHealthCheck,
                0,
                30,
                TimeUnit.SECONDS
        );
    }

    // the shutdown is in the reverse order of startup
    @Override
    public void shutdown() {
        for (Service service: serviceRegistry.getAllServices().reversed()) {
            service.shutdown();
        }
        containerWorkers.shutdownNow();
    }

    // orders all the services to stop accepting new work
    @Override
    public void quiesce() {
        // quiescing the services in reverse order
        for (Service service: serviceRegistry.getAllServices().reversed()) {
            service.quiesce();
        }
        // the container service should be quiesced here

    }

    // gives every service some time to drain themselves
    @Override
    public void drain(long totalTimeout) {
        // draining the services in reverse order
        long individualServiceTimeout = totalTimeout / (serviceRegistry.getSize() + 1); // extra 1 for the container service
        for (Service service: serviceRegistry.getAllServices().reversed()) {
            service.drain(individualServiceTimeout);
        }
    }

    // restarts the service within the given timeout
    private void restartService(Service service, long timeout) {

        if (service == this) return; // cannot restart itself

        service.quiesce();      // stop accepting work
        service.drain(timeout); // drain accepted work
        service.shutdown();     // force shutdown
        service.start();        // start fresh
    }

    // the healthChecker can monitor services in the service registry and report failures to container service
    // after a certain threshold of failures, the container will attempt to restart the affected service
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

    // someone needs to check the health of health checker right? container does it. so if health checker goes down
    // the container service should do something as now we dnt know if our other services are running or not
    // as container doesnt check individually, it relies on health service to report them to container.
    private HealthStatus checkHealthOfHealthChecker() {
        if (!(healthChecker instanceof Service service)) {
            return HealthStatus.UNKNOWN;
        }
        List<HealthReport> healthReports =
                healthChecker.getRecentHealthReports(service);

        if (healthReports == null || healthReports.isEmpty()) {
            return HealthStatus.UNREACHABLE;
        }
        return healthReportAnalyzer.analyze(healthReports);
    }

    // scheduled thread triggers container thread to inspect health service
    private void triggerHealthCheckerHealthCheck() {
        submitToThreadPool(() -> {
            try {
                HealthStatus healthStatus = checkHealthOfHealthChecker();
                evaluateHealthCheckerResponse(healthStatus);
            } catch (Exception exception) {
                ShellLogger.log(
                        "Health checker health evaluation failed: "
                                + exception.getMessage()
                );
            }
        });
    }

    // evaluating the response of health check and deciding if a restart is needed
    private void evaluateHealthCheckerResponse(HealthStatus healthStatus) {
        if (!(healthChecker instanceof Service service)) {
            return;
        }
        if (healthStatus == HealthStatus.HEALTHY) {
            failedServices.remove(service);
            return;
        }
        if (healthStatus == HealthStatus.UNKNOWN) {
            return;
        }
        reportServiceFailure(service);
        if (failedServices.getOrDefault(service, 0) > 3) {
            restartService(service, 1_000);
        }
    }

    @Override
    public HealthReport checkHealth() {
        return null;
    }

    @Override
    public void dispatchCommand(Message command) {
        submitToThreadPool(
                () ->
            messageDispatcher.dispatchResponse(commandHandler.handleCommand(
                    command
            ))
        );
    }

    @Override
    public void start(Service service) {

    }

    @Override
    public void quiesce(Service service) {

    }

    @Override
    public void drain(Service service) {

    }

    @Override
    public void shutdown(Service service) {

    }

    @Override
    public void restart(Service service) {

    }

    private void restartAllServices() {
        List<Service> startupOrder =
                new ArrayList<>(serviceRegistry.getAllServices());
        List<Service> shutdownOrder =
                new ArrayList<>(startupOrder);

        Collections.reverse(shutdownOrder);

        for (Service service : shutdownOrder) {
            service.quiesce();
        }

        for (Service service : shutdownOrder) {
            service.drain(5_000);
        }

        for (Service service : shutdownOrder) {
            service.shutdown();
        }

        for (Service service : startupOrder) {
            try {
                service.start();

                ShellLogger.log(
                        "[ContainerService] Restarted "
                                + service.getClass().getSimpleName()
                );

            } catch (Exception exception) {
                ShellLogger.log(
                        "[ContainerService] Restart failed for "
                                + service.getClass().getSimpleName()
                                + ": " + exception
                );
                exception.printStackTrace();
            }
        }
    }

    @Override
    public void restartAll() {
        submitToThreadPool(
                this::restartAllServices
        );
    }

    @Override
    public List<HealthReport> getHealthReports(Service service) {
        return healthChecker.getRecentHealthReports(service);
    }

    @Override
    public List<HealthReport> getHealthReports() {
        return healthChecker.getRecentHealthReports();
    }

}
