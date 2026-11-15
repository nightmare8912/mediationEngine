package org.prats.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.prats.container.ContainerProcess;
import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.HealthStatus;
import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;
import org.prats.registries.HeartbeatRegistry;
import org.prats.definitions.records.ConsolidatedHealthReport;
import org.prats.utilities.HealthReportFormatter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Duration;
import org.prats.definitions.interfaces.ExecutionContainerManager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExternalHealthMonitor {

    private static final EngineLogger log =
            EngineLogger.getLogger(ExternalHealthMonitor.class);

    private ExecutionContainerManager executionContainerManager;
    private final MessageParser messageParser;
    private final HeartbeatRegistry heartbeatRegistry;
    private final Map<String, Integer> missedHeartbeats;
    private final List<ConsolidatedECConfig> ecConfigsList;
    private final Map<String, ConsolidatedHealthReport> latestReports = new ConcurrentHashMap<>();
    private final ObjectMapper reportMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public String healthReport(String ecName) {
        if (ecConfigsList.stream().noneMatch(ec -> ec.ecId().equals(ecName))) {
            log.warn(() -> "[ExternalHealthMonitor][healthReport] Unknown EC requested | ecName=" + ecName);
            return "Unknown EC: " + ecName;
        }
        var heartbeat = heartbeatRegistry.getLatestHeartbeat(ecName);
        ConsolidatedHealthReport report = latestReports.get(ecName);
        if (heartbeat.isEmpty() || report == null) {
            log.info(() -> "[ExternalHealthMonitor][healthReport] No heartbeat available | ecName=" + ecName);
            return "EC " + ecName + ": UNKNOWN — no heartbeat received. The EC may be stopped or reconnecting.";
        }
        long age = Math.max(0, Duration.between(heartbeat.get().timestamp(), java.time.Instant.now()).getSeconds());
        String freshness = heartbeatRegistry.isExpired(ecName)
                ? "STALE — current health is unknown" : "CURRENT";
        log.info(() -> "[ExternalHealthMonitor][healthReport] EC health report generated"
                + " | ecName=" + ecName + " | freshness=" + freshness
                + " | heartbeatAgeSeconds=" + age + " | reportedStatus=" + report.overallStatus());
        return "EC " + ecName + " heartbeat: " + freshness + " | received " + age + "s ago"
                + "\nReported overall status: " + report.overallStatus()
                + "\nReport timestamp: " + report.timestamp()
                + "\n" + new HealthReportFormatter().format(
                        report.services() == null ? List.of() : List.copyOf(report.services().values()),
                        ecName, report.startedAt());
    }

    private Map<String, ClientConnection> connectedClients;

    public ExternalHealthMonitor(
            HeartbeatRegistry heartbeatRegistry,
            List<ConsolidatedECConfig> ecConfigsList,
            MessageParser messageParser
    ) {
        this.heartbeatRegistry = heartbeatRegistry;
        this.ecConfigsList = ecConfigsList;
        this.messageParser = messageParser;
        this.missedHeartbeats = new ConcurrentHashMap<>();
        this.connectedClients = new ConcurrentHashMap<>();

        log.info(
                "[ExternalHealthMonitor][constructor] External health monitor initialized"
        );
    }

    public void setExecutionContainerManager(
            ExecutionContainerManager executionContainerManager
    ) {
        this.executionContainerManager = executionContainerManager;

        log.debug(() ->
                "[ExternalHealthMonitor][setExecutionContainerManager] "
                        + "Execution container manager configured"
                        + " | managerType="
                        + (executionContainerManager == null
                        ? "null"
                        : executionContainerManager.getClass().getSimpleName())
        );
    }

    public void updateClient(
            String clientName,
            ClientConnection clientConnection
    ) {
        connectedClients.put(clientName, clientConnection);

        log.debug(() ->
                "[ExternalHealthMonitor][updateClient] Client connection updated"
                        + " | clientName=" + clientName
                        + " | connectionType="
                        + (clientConnection == null
                        ? "null"
                        : clientConnection.getClass().getSimpleName())
                        + " | connectedClients=" + connectedClients.size()
        );
    }

    public void register(Message heartbeat) {
        String processName = heartbeat.getProcessName();

        log.debug(() ->
                "[ExternalHealthMonitor][register] Heartbeat received"
                        + " | processName=" + processName
        );

        HealthStatus healthStatus;

        try {
            ConsolidatedHealthReport report = reportMapper.treeToValue(
                    heartbeat.getPayload(), ConsolidatedHealthReport.class);
            if (report == null || report.overallStatus() == null
                    || !processName.equals(report.containerId())
                    || ecConfigsList.stream().noneMatch(ec -> ec.ecId().equals(processName))) {
                log.warn("[ExternalHealthMonitor][register] Ignoring invalid or unconfigured EC heartbeat");
                return;
            }
            healthStatus = report.overallStatus();
            latestReports.put(processName, report);

        } catch (JsonProcessingException jsonProcessingException) {
            log.error(
                    "[ExternalHealthMonitor][register] Unable to decode heartbeat health status"
                            + " | processName=" + processName,
                    jsonProcessingException
            );
            return;
        }

        final HealthStatus registeredHealthStatus = healthStatus;

        log.debug(() ->
                "[ExternalHealthMonitor][register] Registering heartbeat"
                        + " | processName=" + processName
                        + " | healthStatus=" + registeredHealthStatus
        );

        heartbeatRegistry.register(processName, healthStatus);

        log.debug(() ->
                "[ExternalHealthMonitor][register] Heartbeat registered successfully"
                        + " | processName=" + processName
                        + " | healthStatus=" + registeredHealthStatus
        );
    }

    public void verifyHeartbeats() {
        log.debug(
                "[ExternalHealthMonitor][verifyHeartbeats] Heartbeat verification invoked"
        );
    }
}
