package org.prats.handlers;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.MessageType;
import org.prats.definitions.interfaces.ExternalChanelCommunicator;
import org.prats.definitions.interfaces.HealthChecker;
import org.prats.definitions.records.ConsolidatedHealthReport;
import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;

public class ECControlHandler {

    private static final EngineLogger log =
            EngineLogger.getLogger(ECControlHandler.class);

    private final HealthChecker healthChecker;
    private final MessageParser messageParser;

    private ExternalChanelCommunicator externalChanelCommunicator;
    public void handleRequest(Message request) {
        externalChanelCommunicator.sendResponse(messageParser.createErrorResponse(
                request.getCorrelationId(), "UNSUPPORTED_OPERATION", "Unsupported EC control operation."));
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

    public void setControlChannelCommunicator(ExternalChanelCommunicator externalChanelCommunicator) {
        this.externalChanelCommunicator = externalChanelCommunicator;

        log.debug(() ->
                "[ECControlHandler][setControlChannelCommunicator] Control channel communicator configured"
                        + " | communicatorType="
                        + (externalChanelCommunicator == null
                        ? "null"
                        : externalChanelCommunicator.getClass().getSimpleName())
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

        externalChanelCommunicator.sendRequest(heartbeat);

        log.debug(() ->
                "[ECControlHandler][sendHeartbeat] Heartbeat request submitted to control channel"
                        + " | recipientId=" + heartbeat.getRecipientID()
        );
    }
}
