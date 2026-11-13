package org.prats.services.containerServices;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.ContainerType;
import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.definitions.records.ConsolidatedWatchdogConfig;
import org.prats.definitions.records.Platform;
import org.prats.handlers.PlatformCommandHandler;
import org.prats.loggers.FileLogger;
import org.prats.definitions.interfaces.ServiceRegistry;
import org.prats.definitions.interfaces.ExecutionContainerManager;
import org.prats.utilities.HealthReportAnalyzer;
import org.prats.services.ExternalHealthMonitor;
import org.prats.definitions.interfaces.HealthChecker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// the platform service is the manager of the entire distributed system, it has the capabilities of managing its own internal services and the
// external container processes (full jvms). the internal service managing is similar to how the normal container service works.
// however, it has a dedicated server socket (control socket) which it used to monitor its clients. its different from normal tcp sockets.
// all the containers in the system have to send periodic heartbeats of their internal state, the heart-beat would include a overall health status of that container
// platform is not expected to intervene in any of the internals of the container if they report their status as healthy, it is the container's own job do check on its internals
// however, if the platform notices that a container is not sending heart beats, it will send a ping to it. the platform will wait for 5 seconds only to get a response.
// the ping will be sent every 10 seconds for 5 minutes, after which the platform will assume that the container has died and will kill it.
// as of now, all the containers are running on the same machine and we can do a process.destroy() to kill it, however, when they run on different machines its not this simple
// so, there is a host process which is very light weight. it runs on a single thread, and its entire purpose is to launch the container hold its reference.
// it then indefinitely waits for a command from the platform to terminate/re-launch the process and it should have the capability to do it.
// the host on the platform is different, it is a light weight host again with one extra capability, if the platform asks it to re-launch, it can do it.
// it would send repeated periodic heart-beats to platform to check on its health, and would initiate a restart if the platform is unresponsive.
public class PlatformService extends ContainerService implements ExecutionContainerManager {

    private final Platform platform;
    private final ExternalHealthMonitor externalHealthMonitor;
    private PlatformCommandHandler platformCommandHandler;

    private Map<String, ConsolidatedWatchdogConfig> ecWatchdogMap;


    public PlatformService(
            int coreThreads,
            int maxThreads,
            long idleTimeout,
            int tcpPort,
            int tcpBacklog,
            int controlPort,
            String serviceName,
            String databaseConfigFile,
            ServiceRegistry serviceRegistry,
            HealthChecker healthChecker,
            HealthReportAnalyzer healthReportAnalyzer,
            List<ConsolidatedECConfig> configuredEC,
            ExternalHealthMonitor externalHealthMonitor,
            Map<String, ConsolidatedWatchdogConfig> ecWatchdogMap,
            Platform platform
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
                ContainerType.PLATFORM,
                configuredEC,
                serviceRegistry,
                healthChecker,
                healthReportAnalyzer
        );

        this.platform                       = platform;
        this.externalHealthMonitor          = externalHealthMonitor;
        this.ecWatchdogMap                  = ecWatchdogMap;

        if (containerType != ContainerType.PLATFORM)
            throw new IllegalArgumentException(
                    "Container type cannot be picked as EC for Platform config"
            );
    }

    public void setCommandHandler(PlatformCommandHandler platformCommandHandler) {
        this.platformCommandHandler     = platformCommandHandler;
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

    private void setScheduledJobs() {
        scheduleCleanupJobs();
        scheduleMonitoringJobs();
    }

    // scheduled jobs
    private void scheduleCleanupJobs() {

    }

    private void scheduleMonitoringJobs() {
        try {
            scheduledThread.scheduleAtFixedRate(
                    this::triggerHealthCheckerHealthCheck,
                    30,
                    60,
                    TimeUnit.SECONDS
            );
        } catch (Exception exception) {
            FileLogger.log(
                    "[PlatformService] Unable to start triggerHealthCheckerHealthCheck: " + exception.getMessage()
            );
        }
        try {
            // verifying the heartbeats we received
            scheduledThread.scheduleWithFixedDelay(
                    this::triggerVerifyHeartbeats,
                    60,
                    15,
                    TimeUnit.SECONDS
            );
        } catch (Exception exception) {
            FileLogger.log(
                    "[PlatformService] Unable to start verifyHeartbeats : " + exception.getMessage()
            );
        }
    }

    private void triggerVerifyHeartbeats() {
        containerWorkers.submit(
                externalHealthMonitor::verifyHeartbeats
        );
    }


    @Override
    public void notifyMissingHeartbeat(ConsolidatedECConfig ecConfig) {

    }

    private ConsolidatedWatchdogConfig getWatchdog(String ecId) {
        return ecWatchdogMap.get(ecId);
    }

    @Override
    public void startEC(String ecId) {

    }

    @Override
    public void stopEC(String ecId) {

    }

    @Override
    public void restartEC(String ecId) {

    }


    @Override
    public void onRequest(Message request) {

    }

    @Override
    public void onInfo(Message request) {

    }

    @Override
    public void onHeartbeat(Message heartbeat) {
        submitToThreadPool(
                ()-> externalHealthMonitor.register(heartbeat)
        );
    }

    @Override
    public void onResponse(Message response) {

    }

    @Override
    public void onTimeout(Message originalRequest) {

    }

    @Override
    public void dispatchCommand(Message command) {
        submitToThreadPool(
                ()->
                messageDispatcher.dispatchResponse(
                        platformCommandHandler.handleCommand(command)
                )
        );
    }
}
