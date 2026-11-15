package org.prats.handlers;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.MessageType;
import org.prats.definitions.interfaces.ControlChannelCommunicator;
import org.prats.definitions.interfaces.HealthChecker;
import org.prats.definitions.records.ConsolidatedHealthReport;
import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;

public class ECControlHandler {

    private static final EngineLogger log =
            EngineLogger.getLogger(ECControlHandler.class);

    private final HealthChecker healthChecker;
    private final MessageParser messageParser;

    private ControlChannelCommunicator controlChannelCommunicator;
    private JVMDiagnosticsHandler diagnostics;
    public void setDiagnostics(JVMDiagnosticsHandler diagnostics) { this.diagnostics = diagnostics; }
    public void handleRequest(Message request) {
        if (diagnostics != null && (request.getOperation() == org.prats.definitions.enums.Operation.DIAGNOSTICS
                || request.getOperation() == org.prats.definitions.enums.Operation.RUN_GC)) {
            controlChannelCommunicator.sendResponse(diagnostics.handleRequest(request));
        } else {
            controlChannelCommunicator.sendResponse(messageParser.createErrorResponse(
                    request.getCorrelationId(), "UNSUPPORTED_OPERATION", "Unsupported EC control operation."));
        }
    }

    public ECControlHandler(
            HealthChecker healthChecker,
            MessageParser messageParser
    ) {
        this.healthChecker = healthChecker;
        this.messageParser = messageParser;

        log.debug(
                "[ECControlHandler][constructor] EC control handler initialized"
        );
    }

    public void setControlChannelCommunicator(ControlChannelCommunicator controlChannelCommunicator) {
        this.controlChannelCommunicator = controlChannelCommunicator;

        log.debug(() ->
                "[ECControlHandler][setControlChannelCommunicator] Control channel communicator configured"
                        + " | communicatorType="
                        + (controlChannelCommunicator == null
                        ? "null"
                        : controlChannelCommunicator.getClass().getSimpleName())
        );
    }

    public void sendHeartbeat() {

        log.debug(
                "[ECControlHandler][sendHeartbeat] Starting heartbeat generation"
        );

        Message heartbeat = messageParser.createEmptyMessage();
        heartbeat.setMessageType(MessageType.HEARTBEAT);
        heartbeat.setRecipientID("platform");

        ConsolidatedHealthReport report =
                healthChecker.getConsolidatedHealthReport();

        log.debug(() ->
                "[ECControlHandler][sendHeartbeat] Consolidated health report generated"
        );

        heartbeat.setPayload(messageParser.convertToJsonNode(report));

        log.debug(() ->
                "[ECControlHandler][sendHeartbeat] Heartbeat payload populated"
                        + " | messageType=" + heartbeat.getMessageType()
                        + " | recipientId=" + heartbeat.getRecipientID()
        );

        controlChannelCommunicator.sendRequest(heartbeat);

        log.debug(() ->
                "[ECControlHandler][sendHeartbeat] Heartbeat request submitted to control channel"
                        + " | recipientId=" + heartbeat.getRecipientID()
        );
    }
}
