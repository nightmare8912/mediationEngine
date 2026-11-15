
package org.prats.services.containerServices;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.ContainerType;
import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.definitions.records.ConsolidatedWatchdogConfig;
import org.prats.definitions.records.Platform;
import org.prats.handlers.PlatformCommandHandler;
import org.prats.loggers.EngineLogger;
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

    private static final EngineLogger log =
            EngineLogger.getLogger(PlatformService.class);

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

        log.debug(() ->
                "[PlatformService][constructor] Initializing platform service"
                        + " | serviceName=" + serviceName
                        + " | tcpPort=" + tcpPort
                        + " | controlPort=" + controlPort
                        + " | coreThreads=" + coreThreads
                        + " | maxThreads=" + maxThreads
                        + " | idleTimeout=" + idleTimeout
                        + " | configuredECCount="
                        + (configuredEC == null ? "null" : configuredEC.size())
                        + " | watchdogMappingCount="
                        + (ecWatchdogMap == null ? "null" : ecWatchdogMap.size())
        );

        this.platform                       = platform;
        this.externalHealthMonitor          = externalHealthMonitor;
        this.ecWatchdogMap                  = ecWatchdogMap;

        if (containerType != ContainerType.PLATFORM) {

            log.error(
                    "[PlatformService][constructor] Invalid container type for PlatformService"
            );

            throw new IllegalArgumentException(
                    "Container type cannot be picked as EC for Platform config"
            );
        }

        log.info(() ->
                "[PlatformService][constructor] Platform service initialized"
                        + " | serviceName=" + serviceName
                        + " | platformId="
                        + (platform == null ? "null" : platform.id())
                        + " | configuredECCount="
                        + (configuredEC == null ? "null" : configuredEC.size())
                        + " | watchdogMappingCount="
                        + (ecWatchdogMap == null ? "null" : ecWatchdogMap.size())
        );
    }

    public void setCommandHandler(PlatformCommandHandler platformCommandHandler) {

        log.debug("[PlatformService][setCommandHandler] Configuring platform command handler");

        this.platformCommandHandler     = platformCommandHandler;

        log.debug(() ->
                "[PlatformService][setCommandHandler] Platform command handler configured"
                        + " | handler="
                        + (platformCommandHandler == null
                        ? "null"
                        : platformCommandHandler.getClass().getSimpleName())
        );
    }


    @Override
    public void start() {

        log.info(() ->
                "[PlatformService][start] Starting platform service"
                        + " | platformId="
                        + (platform == null ? "null" : platform.id())
        );

        log.debug("[PlatformService][start] Starting parent container service");

        super.start();

        log.info("[PlatformService][start] Parent container service started successfully");

        log.debug("[PlatformService][start] Creating scheduled thread pool");

        scheduledThread = new ScheduledThreadPoolExecutor(1);

        log.info("[PlatformService][start] Scheduled thread pool initialized");

        setScheduledJobs();

        log.info("[PlatformService][start] Platform service startup completed");
    }

    @Override
    public void cleanup() {

        log.debug("[PlatformService][cleanup] Cleanup invoked");
    }

    private void setScheduledJobs() {

        log.debug("[PlatformService][setScheduledJobs] Initializing platform scheduled jobs");

        scheduleCleanupJobs();
        scheduleMonitoringJobs();

        log.debug("[PlatformService][setScheduledJobs] Platform scheduled job initialization completed");
    }

    // scheduled jobs
    private void scheduleCleanupJobs() {

        log.debug("[PlatformService][scheduleCleanupJobs] Cleanup job scheduling invoked");
    }

    private void scheduleMonitoringJobs() {

        log.debug("[PlatformService][scheduleMonitoringJobs] Scheduling platform monitoring jobs");

        try {

            log.debug("[PlatformService][scheduleMonitoringJobs] Scheduling health checker health evaluation"
                    + " | initialDelay=30 seconds"
                    + " | interval=60 seconds");

            scheduledThread.scheduleAtFixedRate(
                    () -> runMaintenance(this::triggerHealthCheckerHealthCheck),
                    30,
                    60,
                    TimeUnit.SECONDS
            );

            log.info("[PlatformService][scheduleMonitoringJobs] Health checker monitoring scheduled successfully");

        } catch (Exception exception) {

            log.error(
                    "[PlatformService][scheduleMonitoringJobs] Unable to start triggerHealthCheckerHealthCheck",
                    exception
            );
        }

        try {

            log.debug("[PlatformService][scheduleMonitoringJobs] Scheduling heartbeat verification"
                    + " | initialDelay=60 seconds"
                    + " | fixedDelay=15 seconds");

            // verifying the heartbeats we received
            scheduledThread.scheduleWithFixedDelay(
                    () -> runMaintenance(this::triggerVerifyHeartbeats),
                    60,
                    15,
                    TimeUnit.SECONDS
            );

            log.info("[PlatformService][scheduleMonitoringJobs] Heartbeat verification scheduled successfully");

        } catch (Exception exception) {

            log.error(
                    "[PlatformService][scheduleMonitoringJobs] Unable to start verifyHeartbeats",
                    exception
            );
        }

        log.debug("[PlatformService][scheduleMonitoringJobs] Monitoring job scheduling sequence completed");
    }

    private void triggerVerifyHeartbeats() {

        log.debug("[PlatformService][triggerVerifyHeartbeats] Heartbeat verification trigger invoked");

        log.debug("[PlatformService][triggerVerifyHeartbeats] Submitting heartbeat verification task");

        containerWorkers.submit(
                externalHealthMonitor::verifyHeartbeats
        );

        log.debug("[PlatformService][triggerVerifyHeartbeats] Heartbeat verification task submitted successfully");
    }


    @Override
    public void notifyMissingHeartbeat(ConsolidatedECConfig ecConfig) {

        log.warn(() ->
                "[PlatformService][notifyMissingHeartbeat] Missing heartbeat notification received"
                        + " | ecId="
                        + (ecConfig == null ? "null" : ecConfig.ecId())
        );
    }

    private ConsolidatedWatchdogConfig getWatchdog(String ecId) {

        log.debug(() ->
                "[PlatformService][getWatchdog] Resolving WATCHDOG configuration"
                        + " | ecId=" + ecId
        );

        ConsolidatedWatchdogConfig watchdogConfig =
                ecWatchdogMap.get(ecId);

        log.debug(() ->
                "[PlatformService][getWatchdog] WATCHDOG configuration lookup completed"
                        + " | ecId=" + ecId
                        + " | found=" + (watchdogConfig != null)
        );

        return watchdogConfig;
    }

    @Override
    public void startEC(String ecId) {

        log.debug(() ->
                "[PlatformService][startEC] EC start operation invoked"
                        + " | ecId=" + ecId
        );
    }

    @Override
    public void stopEC(String ecId) {

        log.debug(() ->
                "[PlatformService][stopEC] EC stop operation invoked"
                        + " | ecId=" + ecId
        );
    }

    @Override
    public void restartEC(String ecId) {

        log.debug(() ->
                "[PlatformService][restartEC] EC restart operation invoked"
                        + " | ecId=" + ecId
        );
    }


    @Override
    public void onRequest(Message request) {

        log.debug("[PlatformService][onRequest] Request handler invoked");
    }

    @Override
    public void onInfo(Message request) {

        log.debug("[PlatformService][onInfo] Information handler invoked");
    }

    @Override
    public void onHeartbeat(Message heartbeat) {

        log.debug(() ->
                "[PlatformService][onHeartbeat] Heartbeat received"
                        + " | messageType="
                        + (heartbeat == null
                        ? "null"
                        : heartbeat.getClass().getSimpleName())
        );

        log.debug("[PlatformService][onHeartbeat] Submitting heartbeat registration task");

        submitToThreadPool(
                () -> {

                    log.debug("[PlatformService][onHeartbeat] Heartbeat registration task started");

                    externalHealthMonitor.register(heartbeat);

                    log.debug("[PlatformService][onHeartbeat] Heartbeat registration completed");
                }
        );

        log.debug("[PlatformService][onHeartbeat] Heartbeat registration task submitted");
    }

    @Override
    public void onResponse(Message response) {

        log.debug("[PlatformService][onResponse] Response handler invoked");
    }

    @Override
    public void onTimeout(Message originalRequest) {

        log.debug("[PlatformService][onTimeout] Timeout handler invoked");
    }

    @Override
    public void dispatchCommand(Message command) {

        log.debug(() ->
                "[PlatformService][dispatchCommand] Platform command received for asynchronous processing"
                        + " | messageType="
                        + (command == null
                        ? "null"
                        : command.getClass().getSimpleName())
        );

        submitToThreadPool(
                () -> {

                    log.debug("[PlatformService][dispatchCommand] Command processing task started");

                    Message response = platformCommandHandler.handleCommand(command);
                    if (response != null) {
                        messageDispatcher.dispatchResponse(response);
                    }

                    log.debug("[PlatformService][dispatchCommand] Command response dispatched successfully");
                }
        );

        log.debug("[PlatformService][dispatchCommand] Command processing task submitted");
    }
}
