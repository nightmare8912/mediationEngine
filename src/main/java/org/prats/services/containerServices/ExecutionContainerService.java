
package org.prats.services.containerServices;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.ContainerType;
import org.prats.definitions.interfaces.TCPMessageHandler;
import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.handlers.ECControlHandler;
import org.prats.handlers.ExecutionContainerCommandHandler;
import org.prats.loggers.EngineLogger;
import org.prats.definitions.interfaces.ServiceRegistry;
import org.prats.definitions.interfaces.HealthChecker;
import org.prats.utilities.HealthReportAnalyzer;

import java.util.List;

import java.util.concurrent.*;


// the container service is a lifecycle manager of all the services that run inside the container(jvm),
// it can decide when to start drain or quiesce a service. no other service inside the jvm should have this capability
// if the container service detects that the jvm is not healthy, it can do its own re-initialization
// however if that fails, it will ask the platform to kill itself and restart again
// if that fails as well, the platform should be able to detect that container service unresponsive and
// initiate its restart, only an external jvm which runs the platform jvm can kill this service
public class ExecutionContainerService extends ContainerService implements TCPMessageHandler {

    private static final EngineLogger log =
            EngineLogger.getLogger(ExecutionContainerService.class);

    private final ECControlHandler ecControlHandler;

    private ExecutionContainerCommandHandler executionContainerCommandHandler;

    public ExecutionContainerService(
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
            HealthReportAnalyzer healthReportAnalyzer,
            ECControlHandler ecControlHandler
    ) {

        super(
                coreThreads,
                maxThreads,
                idleTimeout,
                tcpPort,
                tcpBacklog,
                controlPort,
                serviceName,
                databaseConfigFile,
                containerType,
                configuredEC,
                serviceRegistry,
                healthChecker,
                healthReportAnalyzer
        );

        log.debug(() ->
                "[ExecutionContainerService][constructor] Initializing execution container service"
                        + " | serviceName=" + serviceName
                        + " | containerType=" + containerType
                        + " | tcpPort=" + tcpPort
                        + " | controlPort=" + controlPort
                        + " | coreThreads=" + coreThreads
                        + " | maxThreads=" + maxThreads
                        + " | idleTimeout=" + idleTimeout
                        + " | configuredECCount="
                        + (configuredEC == null ? "null" : configuredEC.size())
                        + " | ecControlHandler="
                        + (ecControlHandler == null
                        ? "null"
                        : ecControlHandler.getClass().getSimpleName())
        );

        this.ecControlHandler       = ecControlHandler;

        if (containerType == ContainerType.PLATFORM) {

            log.error(
                    "[ExecutionContainerService][constructor] Invalid container type: PLATFORM"
            );

            throw new IllegalArgumentException(
                    "Container type cannot be picked as platform for EC config"
            );
        }

        log.info(() ->
                "[ExecutionContainerService][constructor] Execution container service initialized"
                        + " | serviceName=" + serviceName
                        + " | containerType=" + containerType
        );
    }

    public void setCommandHandler(ExecutionContainerCommandHandler executionContainerCommandHandler) {

        log.debug("[ExecutionContainerService][setCommandHandler] Configuring execution container command handler");

        this.executionContainerCommandHandler = executionContainerCommandHandler;

        log.debug(() ->
                "[ExecutionContainerService][setCommandHandler] Command handler configured"
                        + " | handler="
                        + (executionContainerCommandHandler == null
                        ? "null"
                        : executionContainerCommandHandler.getClass().getSimpleName())
        );
    }

    private void setScheduledJobs() {

        log.debug("[ExecutionContainerService][setScheduledJobs] Initializing scheduled jobs");

        // now we start check for health checker as it should have been started by now
        scheduleMonitoringJobs(); // sets schedule for monitoring jobs
        scheduleCleanupJobs(); // sets schedule for cleanup jobs

        log.debug("[ExecutionContainerService][setScheduledJobs] Scheduled job initialization completed");
    }

    @Override
    public void start() {

        log.info(() ->
                "[ExecutionContainerService][start] Starting execution container service"
                        + " | serviceName=" + serviceName
        );

        log.debug("[ExecutionContainerService][start] Starting parent container service");

        super.start();

        log.info("[ExecutionContainerService][start] Parent container service started successfully");

        log.debug("[ExecutionContainerService][start] Creating scheduled thread pool");

        scheduledThread = new ScheduledThreadPoolExecutor(1);

        log.info("[ExecutionContainerService][start] Scheduled thread pool initialized");

        setScheduledJobs();

        log.info("[ExecutionContainerService][start] Execution container service startup completed");
    }

    @Override
    public void cleanup() {

        log.debug("[ExecutionContainerService][cleanup] Cleanup requested");
    }

    // scheduled jobs
    private void scheduleCleanupJobs() {

        log.debug("[ExecutionContainerService][scheduleCleanupJobs] Cleanup job scheduling invoked");
    }

    private void scheduleMonitoringJobs() {

        log.debug("[ExecutionContainerService][scheduleMonitoringJobs] Scheduling health monitoring and heartbeat jobs");

        //  check for health checker as it should have been started by now
        try {

            log.debug("[ExecutionContainerService][scheduleMonitoringJobs] Scheduling health checker evaluation"
                    + " | initialDelay=0 | interval=30 seconds");

            scheduledThread.scheduleAtFixedRate(
                    () -> runMaintenance(this::triggerHealthCheckerHealthCheck),
                    0,
                    30,
                    TimeUnit.SECONDS
            );

            log.info("[ExecutionContainerService][scheduleMonitoringJobs] Health checker monitoring scheduled successfully");

            log.debug("[ExecutionContainerService][scheduleMonitoringJobs] Scheduling heartbeat"
                    + " | initialDelay=15 seconds | interval=15 seconds");

            scheduledThread.scheduleAtFixedRate(
                    () -> runMaintenance(this::triggerHeartbeat),
                    15,
                    15,
                    TimeUnit.SECONDS
            );

            log.info("[ExecutionContainerService][scheduleMonitoringJobs] Heartbeat scheduling completed successfully");

        } catch (Exception exception) {

            log.error(
                    "[ExecutionContainerService][scheduleMonitoringJobs] Unable to start scheduled jobs",
                    exception
            );
        }
    }

    @Override
    public void dispatchCommand(Message command) {

        log.debug(() ->
                "[ExecutionContainerService][dispatchCommand] Command received for asynchronous processing"
                        + " | command=" + command
        );

        submitToThreadPool(
                () -> {

                    log.debug("[ExecutionContainerService][dispatchCommand] Command processing task started");

                    messageDispatcher.dispatchResponse(executionContainerCommandHandler.handleCommand(
                            command
                    ));

                    log.debug("[ExecutionContainerService][dispatchCommand] Command processing and response dispatch completed");
                }
        );

        log.debug("[ExecutionContainerService][dispatchCommand] Command task submitted");
    }


    @Override
    public void onRequest(Message request) {

        submitToThreadPool(() -> ecControlHandler.handleRequest(request));
    }

    @Override
    public void onInfo(Message request) {

        log.debug("[ExecutionContainerService][onInfo] Information handler invoked; no information handling implemented");
    }

    private void triggerHeartbeat() {

        log.debug("[ExecutionContainerService][triggerHeartbeat] Heartbeat trigger invoked");

        submitToThreadPool(
                () -> {

                    log.debug("[ExecutionContainerService][triggerHeartbeat] Sending heartbeat");

                    ecControlHandler.sendHeartbeat();

                    log.debug("[ExecutionContainerService][triggerHeartbeat] Heartbeat send operation completed");
                }
        );

        log.debug("[ExecutionContainerService][triggerHeartbeat] Heartbeat task submitted to container workers");
    }

    @Override
    public void onHeartbeat(Message response) {

        log.warn(() ->
                "[ExecutionContainerService][onHeartbeat] Execution container service received a heartbeat, it shouldn't. Ignoring. Heartbeat: "
                        + response
        );
    }

    @Override
    public void onResponse(Message response) {

        log.debug("[ExecutionContainerService][onResponse] Response handler invoked; no response handling implemented");
    }

    @Override
    public void onTimeout(Message originalRequest, Throwable cause) {

        log.debug("[ExecutionContainerService][onTimeout] Timeout handler invoked; no timeout handling implemented");
    }
}
