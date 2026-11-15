
package org.prats.services.containerServices;

import org.prats.definitions.enums.ContainerType;
import org.prats.definitions.enums.HealthStatus;
import org.prats.definitions.interfaces.*;
import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.definitions.records.HealthReport;
import org.prats.loggers.EngineLogger;
import org.prats.definitions.interfaces.ServiceRegistry;
import org.prats.definitions.interfaces.HealthChecker;
import org.prats.threading.ThreadPool;
import org.prats.utilities.HealthReportAnalyzer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public abstract class ContainerService implements Service, ServiceStatusReporter, CommandDispatcher, ServiceManager, HealthCheckable, TCPMessageHandler {

    private static final EngineLogger log =
            EngineLogger.getLogger(ContainerService.class);

    protected final ContainerType containerType;
    protected final ServiceRegistry serviceRegistry;
    protected final HealthChecker healthChecker;
    protected final HealthReportAnalyzer healthReportAnalyzer;
    protected final Map<Service, List<HealthReport>> recentFailureReports;
    public final List<ConsolidatedECConfig> configuredEC;

    protected final int tcpPort, tcpBacklog, controlPort, coreThreads, maxThreads;
    protected final long idleTimeout;
    protected final String serviceName;
    protected final String databaseConfigFile;
    protected final Map<Service, Integer> failedServices;

    protected ExecutorService containerWorkers;
    protected MessageDispatcher messageDispatcher;
    protected ExternalChanelCommunicator externalChanelCommunicator;

    protected volatile ScheduledExecutorService scheduledThread;

    public ContainerService(
            int coreThreads,
            int maxThreads,
            long idleTimeout,
            int tcpPort,
            int tcpBacklog,
            int controlPort,
            String serviceName,
            String databaseConfigFile,
            ContainerType containerType,
            List<ConsolidatedECConfig> configuredEC,
            ServiceRegistry serviceRegistry,
            HealthChecker healthChecker,
            HealthReportAnalyzer healthReportAnalyzer
    ) {

        log.debug(() ->
                "[ContainerService][constructor] Initializing container service"
                        + " | serviceName=" + serviceName
                        + " | containerType=" + containerType
                        + " | coreThreads=" + coreThreads
                        + " | maxThreads=" + maxThreads
                        + " | idleTimeout=" + idleTimeout
                        + " | tcpPort=" + tcpPort
                        + " | tcpBacklog=" + tcpBacklog
                        + " | controlPort=" + controlPort
                        + " | configuredECCount="
                        + (configuredEC == null ? "null" : configuredEC.size())
        );

        this.coreThreads                = coreThreads;
        this.maxThreads                 = maxThreads;
        this.idleTimeout                = idleTimeout;

        this.tcpPort                    = tcpPort;
        this.tcpBacklog                 = tcpBacklog;
        this.controlPort                = controlPort;
        this.serviceName                = serviceName;
        this.databaseConfigFile         = databaseConfigFile;

        this.containerType              = containerType;
        this.configuredEC               = configuredEC;
        this.serviceRegistry            = serviceRegistry;
        this.healthChecker              = healthChecker;
        this.healthReportAnalyzer       = healthReportAnalyzer;

        this.failedServices             = new ConcurrentHashMap<>();
        this.recentFailureReports       = new ConcurrentHashMap<>();

        log.info(() ->
                "[ContainerService][constructor] Container service initialized"
                        + " | serviceName=" + serviceName
                        + " | containerType=" + containerType
        );
    }

    public void setMessageDispatcher(MessageDispatcher messageDispatcher) {

        log.debug("[ContainerService][setMessageDispatcher] Updating message dispatcher");

        this.messageDispatcher = messageDispatcher;

        log.debug("[ContainerService][setMessageDispatcher] Message dispatcher configured");
    }

    public void setControlChannelCommunicator(ExternalChanelCommunicator externalChanelCommunicator) {

        log.debug("[ContainerService][setControlChannelCommunicator] Updating control channel communicator");

        this.externalChanelCommunicator = externalChanelCommunicator;

        log.debug("[ContainerService][setControlChannelCommunicator] Control channel communicator configured");
    }

    // through this function  health services can report to the container service about the services that are not running
    public void reportServiceFailure(Service service) {

        log.debug(() ->
                "[ContainerService][reportServiceFailure] Recording service failure"
                        + " | service="
                        + (service == null ? "null" : service.getClass().getSimpleName())
        );

        failedServices.merge(service, 1, Integer::sum);

        log.warn(() ->
                "[ContainerService][reportServiceFailure] Service failure recorded"
                        + " | service=" + service.getClass().getSimpleName()
                        + " | failureCount=" + failedServices.get(service)
        );
    }

    // submits task to platform workers
    protected void submitToThreadPool(Runnable runnable) {

        log.debug("[ContainerService][submitToThreadPool] Submitting task to container workers");

        containerWorkers.submit(
                runnable
        );

        log.debug("[ContainerService][submitToThreadPool] Task submitted successfully");
    }

    protected void runMaintenance(Runnable trigger) {
        try { trigger.run(); }
        catch (java.util.concurrent.RejectedExecutionException exception) {
            log.warn("[ContainerService][runMaintenance] Maintenance submission rejected; next scheduled cycle will retry");
        } catch (RuntimeException exception) {
            log.error("[ContainerService][runMaintenance] Maintenance trigger failed", exception);
        }
    }


    @Override
    public void start() {

        log.info(() ->
                "[ContainerService][start] Starting container services"
                        + " | serviceName=" + serviceName
                        + " | containerType=" + containerType
        );

        // start the container first
        log.debug("[ContainerService][start] Initializing container worker thread pool");

        containerWorkers       = new ThreadPool().initializeAndGetThreadPool(
                coreThreads, maxThreads, idleTimeout, true
        );

        log.info("[ContainerService][start] Container worker thread pool initialized");

        // the services were inserted in an order in which we want them to start, and we iterate them and start in the same order
        for (Service service : serviceRegistry.getAllServices()) {

            String serviceName = service.getClass().getSimpleName();

            log.info(() ->
                    "[PlatformInitializer] Starting service: " + serviceName
            );

            log.debug(() ->
                    "[ContainerService][start] Invoking service.start()"
                            + " | service=" + serviceName
            );

            try {

                service.start();

                log.info(() ->
                        "[PlatformInitializer] Successfully started: " + serviceName
                );

                log.debug(() ->
                        "[ContainerService][start] Service startup completed"
                                + " | service=" + serviceName
                );

            } catch (Exception exception) {
                // logFailedService(service);
                // the service didn't start, not good... had put exception handling here as one service going down
                // should not make other services not start as well. we expect that healthService will log this to
                // container service and container service will do a recovery of the failed service later
                // however i doubt how much that recovery would be useful

                log.error(
                        "[ContainerService] Failed to start "
                                + service.getClass().getSimpleName(),
                        exception
                );
            }
        }

        log.info("[ContainerService][start] Container service startup sequence completed");
    }

    // the shutdown is in the reverse order of startup
    @Override
    public void shutdown() {

        log.info("[ContainerService][shutdown] Initiating container shutdown");

        for (Service service: serviceRegistry.getAllServices().reversed()) {

            log.debug(() ->
                    "[ContainerService][shutdown] Shutting down service"
                            + " | service=" + service.getClass().getSimpleName()
            );

            service.shutdown();

            log.debug(() ->
                    "[ContainerService][shutdown] Service shutdown completed"
                            + " | service=" + service.getClass().getSimpleName()
            );
        }

        log.debug("[ContainerService][shutdown] Shutting down container worker pool");

        containerWorkers.shutdownNow();

        log.info("[ContainerService][shutdown] Container shutdown completed");
    }

    // orders all the services to stop accepting new work
    @Override
    public void quiesce() {

        log.info("[ContainerService][quiesce] Initiating container quiescence");

        // quiescing the services in reverse order
        for (Service service: serviceRegistry.getAllServices().reversed()) {

            log.debug(() ->
                    "[ContainerService][quiesce] Quiescing service"
                            + " | service=" + service.getClass().getSimpleName()
            );

            service.quiesce();

            log.debug(() ->
                    "[ContainerService][quiesce] Service quiesced"
                            + " | service=" + service.getClass().getSimpleName()
            );
        }

        // the container service should be quiesced here

        log.info("[ContainerService][quiesce] Container quiescence completed");
    }

    // gives every service some time to drain themselves
    @Override
    public void drain(long totalTimeout) {

        log.info(() ->
                "[ContainerService][drain] Initiating container drain"
                        + " | totalTimeout=" + totalTimeout
        );

        // draining the services in reverse order
        long individualServiceTimeout = totalTimeout / (serviceRegistry.getSize() + 1); // extra 1 for the container service

        log.debug(() ->
                "[ContainerService][drain] Calculated individual service timeout"
                        + " | individualServiceTimeout=" + individualServiceTimeout
                        + " | serviceCount=" + serviceRegistry.getSize()
        );

        for (Service service: serviceRegistry.getAllServices().reversed()) {

            log.debug(() ->
                    "[ContainerService][drain] Draining service"
                            + " | service=" + service.getClass().getSimpleName()
                            + " | timeout=" + individualServiceTimeout
            );

            service.drain(individualServiceTimeout);

            log.debug(() ->
                    "[ContainerService][drain] Service drain completed"
                            + " | service=" + service.getClass().getSimpleName()
            );
        }

        log.info("[ContainerService][drain] Container drain completed");
    }

    // restarts the service within the given timeout
    protected void restartService(Service service, long timeout) {

        log.info(() ->
                "[ContainerService][restartService] Restart requested"
                        + " | service="
                        + (service == null ? "null" : service.getClass().getSimpleName())
                        + " | timeout=" + timeout
        );

        if (service == this) {
            log.warn("[ContainerService][restartService] Cannot restart the container service itself");
            return; // cannot restart itself
        }

        log.debug("[ContainerService][restartService] Quiescing service");

        service.quiesce();      // stop accepting work

        log.debug("[ContainerService][restartService] Service quiesced; draining accepted work");

        service.drain(timeout); // drain accepted work

        log.debug("[ContainerService][restartService] Service drained; shutting it down");

        service.shutdown();     // force shutdown

        log.debug("[ContainerService][restartService] Service shutdown completed; starting fresh");

        service.start();        // start fresh

        log.info(() ->
                "[ContainerService][restartService] Service restart completed"
                        + " | service=" + service.getClass().getSimpleName()
        );
    }

    // the healthChecker can monitor services in the service registry and report failures to container service
    // after a certain threshold of failures, the container will attempt to restart the affected service
    @Override
    public void reportServiceFailure(
            Service service,
            List<HealthReport> recentHealthReports
    ) {

        log.debug(() ->
                "[ContainerService][reportServiceFailure] Recording service failure with health reports"
                        + " | service=" + service.getClass().getSimpleName()
                        + " | reportCount="
                        + (recentHealthReports == null ? "null" : recentHealthReports.size())
        );

        reportServiceFailure(service);

        recentFailureReports.put(
                service,
                List.copyOf(recentHealthReports)
        );

        log.debug(() ->
                "[ContainerService][reportServiceFailure] Recent failure reports stored"
                        + " | service=" + service.getClass().getSimpleName()
                        + " | reportCount=" + recentFailureReports.get(service).size()
        );
    }

    // the health services can report to the container that the failed service is now back up so it can stop monitoring
    @Override
    public void reportServiceRecovered(Service service) {

        log.info(() ->
                "[ContainerService][reportServiceRecovered] Service recovery reported"
                        + " | service=" + service.getClass().getSimpleName()
        );

        failedServices.remove(service);
        recentFailureReports.remove(service);

        log.info(() ->
                "[ContainerService][reportServiceRecovered] Service failure tracking cleared"
                        + " | service=" + service.getClass().getSimpleName()
        );
    }

    // someone needs to check the health of health checker right? container does it. so if health checker goes down
    // the container service should do something as now we dnt know if our other services are running or not
    // as container doesnt check individually, it relies on health service to report them to container.
    protected HealthStatus checkHealthOfHealthChecker() {

        log.debug("[ContainerService][checkHealthOfHealthChecker] Checking health of health checker");

        if (!(healthChecker instanceof Service service)) {

            log.debug("[ContainerService][checkHealthOfHealthChecker] Health checker is not a Service; returning UNKNOWN");

            return HealthStatus.UNKNOWN;
        }

        log.debug(() ->
                "[ContainerService][checkHealthOfHealthChecker] Retrieving recent health reports"
                        + " | service=" + service.getClass().getSimpleName()
        );

        List<HealthReport> healthReports =
                healthChecker.getRecentHealthReports(service);

        if (healthReports == null || healthReports.isEmpty()) {

            log.warn("[ContainerService][checkHealthOfHealthChecker] No health reports available; returning UNREACHABLE");

            return HealthStatus.UNREACHABLE;
        }

        log.debug(() ->
                "[ContainerService][checkHealthOfHealthChecker] Health reports retrieved"
                        + " | reportCount=" + healthReports.size()
        );

        HealthStatus healthStatus = healthReportAnalyzer.analyze(healthReports);

        log.debug(() ->
                "[ContainerService][checkHealthOfHealthChecker] Health analysis completed"
                        + " | healthStatus=" + healthStatus
        );

        return healthStatus;
    }

    // scheduled thread triggers container thread to inspect health service
    protected void triggerHealthCheckerHealthCheck() {

        log.debug("[ContainerService][triggerHealthCheckerHealthCheck] Scheduling health checker evaluation");

        submitToThreadPool(() -> {

            log.debug("[ContainerService][triggerHealthCheckerHealthCheck] Health evaluation task started");

            try {

                HealthStatus healthStatus = checkHealthOfHealthChecker();

                log.debug(() ->
                        "[ContainerService][triggerHealthCheckerHealthCheck] Health evaluation returned"
                                + " | healthStatus=" + healthStatus
                );

                evaluateHealthCheckerResponse(healthStatus);

                log.debug("[ContainerService][triggerHealthCheckerHealthCheck] Health evaluation task completed");

            } catch (Exception exception) {

                log.error(
                        "[ContainerService][triggerHealthCheckerHealthCheck] Health checker health evaluation failed",
                        exception
                );
            }
        });
    }

    // evaluating the response of health check and deciding if a restart is needed
    protected void evaluateHealthCheckerResponse(HealthStatus healthStatus) {

        log.debug(() ->
                "[ContainerService][evaluateHealthCheckerResponse] Evaluating health checker response"
                        + " | healthStatus=" + healthStatus
        );

        if (!(healthChecker instanceof Service service)) {

            log.debug("[ContainerService][evaluateHealthCheckerResponse] Health checker is not a Service; no action required");

            return;
        }

        if (healthStatus == HealthStatus.HEALTHY) {

            failedServices.remove(service);

            log.info(() ->
                    "[ContainerService][evaluateHealthCheckerResponse] Health checker is healthy"
                            + " | service=" + service.getClass().getSimpleName()
            );

            return;
        }

        if (healthStatus == HealthStatus.UNKNOWN) {

            log.debug("[ContainerService][evaluateHealthCheckerResponse] Health status UNKNOWN; no recovery action taken");

            return;
        }

        reportServiceFailure(service);

        int failureCount = failedServices.getOrDefault(service, 0);

        log.warn(() ->
                "[ContainerService][evaluateHealthCheckerResponse] Health checker is not healthy"
                        + " | service=" + service.getClass().getSimpleName()
                        + " | healthStatus=" + healthStatus
                        + " | failureCount=" + failureCount
        );

        if (failureCount > 3) {

            log.warn(() ->
                    "[ContainerService][evaluateHealthCheckerResponse] Restart threshold exceeded"
                            + " | service=" + service.getClass().getSimpleName()
                            + " | failureCount=" + failureCount
            );

            restartService(service, 1_000);
        }
    }

    @Override
    public HealthReport checkHealth() {

        log.debug("[ContainerService][checkHealth] Default checkHealth implementation invoked");

        return null;
    }

    @Override
    public void start(Service service) {

        log.debug(() ->
                "[ContainerService][start(Service)] Service start command received"
                        + " | service="
                        + (service == null ? "null" : service.getClass().getSimpleName())
        );
    }

    @Override
    public void quiesce(Service service) {

        log.debug(() ->
                "[ContainerService][quiesce(Service)] Service quiesce command received"
                        + " | service="
                        + (service == null ? "null" : service.getClass().getSimpleName())
        );
    }

    @Override
    public void drain(Service service) {

        log.debug(() ->
                "[ContainerService][drain(Service)] Service drain command received"
                        + " | service="
                        + (service == null ? "null" : service.getClass().getSimpleName())
        );
    }

    @Override
    public void shutdown(Service service) {

        log.debug(() ->
                "[ContainerService][shutdown(Service)] Service shutdown command received"
                        + " | service="
                        + (service == null ? "null" : service.getClass().getSimpleName())
        );
    }

    @Override
    public void restart(Service service) {

        log.debug(() ->
                "[ContainerService][restart(Service)] Service restart command received"
                        + " | service="
                        + (service == null ? "null" : service.getClass().getSimpleName())
        );
    }

    private void restartAllServices() {

        log.info("[ContainerService][restartAllServices] Initiating restart of all services");

        List<Service> startupOrder =
                new ArrayList<>(serviceRegistry.getAllServices());

        List<Service> shutdownOrder =
                new ArrayList<>(startupOrder);

        Collections.reverse(shutdownOrder);

        log.debug(() ->
                "[ContainerService][restartAllServices] Restart order prepared"
                        + " | serviceCount=" + startupOrder.size()
        );

        for (Service service : shutdownOrder) {

            log.debug(() ->
                    "[ContainerService][restartAllServices] Quiescing service"
                            + " | service=" + service.getClass().getSimpleName()
            );

            service.quiesce();
        }

        for (Service service : shutdownOrder) {

            log.debug(() ->
                    "[ContainerService][restartAllServices] Draining service"
                            + " | service=" + service.getClass().getSimpleName()
                            + " | timeout=5000"
            );

            service.drain(5_000);
        }

        for (Service service : shutdownOrder) {

            log.debug(() ->
                    "[ContainerService][restartAllServices] Shutting down service"
                            + " | service=" + service.getClass().getSimpleName()
            );

            service.shutdown();
        }

        for (Service service : startupOrder) {

            try {

                log.debug(() ->
                        "[ContainerService][restartAllServices] Starting service"
                                + " | service=" + service.getClass().getSimpleName()
                );

                service.start();

                log.info(() ->
                        "[ContainerService] Restarted "
                                + service.getClass().getSimpleName()
                );

            } catch (Exception exception) {

                log.error(
                        "[ContainerService] Restart failed for "
                                + service.getClass().getSimpleName(),
                        exception
                );

            }
        }

        log.info("[ContainerService][restartAllServices] Restart sequence completed");
    }

    @Override
    public void restartAll() {

        log.info("[ContainerService][restartAll] Request to restart all services received");

        submitToThreadPool(
                this::restartAllServices
        );

        log.debug("[ContainerService][restartAll] Restart task submitted");
    }

    @Override
    public List<HealthReport> getHealthReports(Service service) {

        log.debug(() ->
                "[ContainerService][getHealthReports] Retrieving service health reports"
                        + " | service="
                        + (service == null ? "null" : service.getClass().getSimpleName())
        );

        List<HealthReport> healthReports =
                healthChecker.getRecentHealthReports(service);

        log.debug(() ->
                "[ContainerService][getHealthReports] Service health reports retrieved"
                        + " | reportCount="
                        + (healthReports == null ? "null" : healthReports.size())
        );

        return healthReports;
    }

    @Override
    public List<HealthReport> getHealthReports() {

        log.debug("[ContainerService][getHealthReports] Retrieving all health reports");

        List<HealthReport> healthReports =
                healthChecker.getRecentHealthReports();

        log.debug(() ->
                "[ContainerService][getHealthReports] Health reports retrieved"
                        + " | reportCount="
                        + (healthReports == null ? "null" : healthReports.size())
        );

        return healthReports;
    }
}
