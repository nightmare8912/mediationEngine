package org.prats.services.runtimeService;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.ContainerType;
import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.definitions.records.EC;
import org.prats.handlers.ExecutionContainerCommandHandler;
import org.prats.loggers.FileLogger;
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
public class ExecutionContainerService extends ContainerService {

    private ExecutionContainerCommandHandler executionContainerCommandHandler;
    private final EC ecConfig;

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
            EC ecConfig
            // storing a reference here as the container service may decide to do a full restart of all the services using itself if
            // it decided that is appropriate, the restart of services not the entire jvm
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

        this.ecConfig     = ecConfig;

        if (containerType == ContainerType.PLATFORM)
            throw new IllegalArgumentException(
                    "Container type cannot be picked as platform for EC config"
            );
    }

    public void setCommandHandler(ExecutionContainerCommandHandler executionContainerCommandHandler) {
        this.executionContainerCommandHandler = executionContainerCommandHandler;
    }

    private void setScheduledJobs() {
        // now we start check for health checker as it should have been started by now
        scheduleMonitoringJobs(); // sets schedule for monitoring jobs
        scheduleCleanupJobs(); // sets schedule for cleanup jobs
    }

    @Override
    public void start() {
        super.start();
        scheduledThread = new ScheduledThreadPoolExecutor(1);
        setScheduledJobs();
    }

    @Override
    public void cleanup() {

    }

    // scheduled jobs
    private void scheduleCleanupJobs() {

    }

    private void scheduleMonitoringJobs() {
        //  check for health checker as it should have been started by now
        try {
            scheduledThread.scheduleAtFixedRate(
                    this::triggerHealthCheckerHealthCheck,
                    0,
                    30,
                    TimeUnit.SECONDS
            );
        } catch (Exception exception) {
            FileLogger.log(
                    "[ContainerService] Unable to start triggerHealthCheckerHealthCheck: " + exception.getMessage()
            );
        }
    }

    @Override
    public void dispatchCommand(Message command) {
        submitToThreadPool(
                () ->
                        messageDispatcher.dispatchResponse(executionContainerCommandHandler.handleCommand(
                                command
                        ))
        );
    }


    @Override
    public void onRequest(Message request) {

    }

    @Override
    public void onInfo(Message request) {

    }

    @Override
    public void onHeartbeat(Message response) {

    }

    @Override
    public void onResponse(Message response) {

    }

    @Override
    public void onTimeout(Message originalRequest) {

    }
}
