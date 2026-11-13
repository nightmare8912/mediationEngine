package org.prats.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.prats.container.ContainerProcess;
import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.HealthStatus;
import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.loggers.FileLogger;
import org.prats.parsers.MessageParser;
import org.prats.registries.HeartbeatRegistry;
import org.prats.definitions.interfaces.ExecutionContainerManager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExternalHealthMonitor {

    private ExecutionContainerManager executionContainerManager;
    private final MessageParser messageParser;
    private final HeartbeatRegistry heartbeatRegistry;
    private final Map<String, Integer> missedHeartbeats;
    private final List<ConsolidatedECConfig> ecConfigsList;


    private Map<String, ClientConnection> connectedClients;

    public ExternalHealthMonitor(
            HeartbeatRegistry heartbeatRegistry,
            List<ConsolidatedECConfig> ecConfigsList,
            MessageParser messageParser
    ) {
        this.heartbeatRegistry      = heartbeatRegistry;
        this.ecConfigsList          = ecConfigsList;
        this.messageParser          = messageParser;
        this.missedHeartbeats       = new ConcurrentHashMap<>();
        this.connectedClients       = new ConcurrentHashMap<>();
    }

    public void setExecutionContainerManager(ExecutionContainerManager executionContainerManager) {
        this.executionContainerManager = executionContainerManager;
    }

    public void updateClient(
            String clientName,
            ClientConnection clientConnection
    ) {
        connectedClients.put(clientName, clientConnection);
    }

    public  void register(Message heartbeat) {
        String processName          = heartbeat.getProcessName();
        HealthStatus healthStatus   = HealthStatus.INVALID;
        try {
            healthStatus = messageParser.extractPayloadField(
                    heartbeat,
                    "status",
                    HealthStatus.class
            );
        } catch (JsonProcessingException jsonProcessingException) {
            FileLogger.log(
                    "[ExternalHealthMonitor] [updateClient] JSONDecodingException: " + jsonProcessingException.getMessage()
            );
        }
        heartbeatRegistry.register(processName, healthStatus);
    }

    public void verifyHeartbeats() {

    }
}
