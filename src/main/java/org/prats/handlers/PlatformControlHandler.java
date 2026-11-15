package org.prats.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.interfaces.ExternalChanelCommunicator;
import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.definitions.records.ConsolidatedWatchdogConfig;
import org.prats.loggers.EngineLogger;
import org.prats.parsers.CommandParser;
import org.prats.replyChannels.FutureReplyChannel;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public class PlatformControlHandler {

    private static final EngineLogger log =
            EngineLogger.getLogger(PlatformControlHandler.class);

    private ExternalChanelCommunicator externalChanelCommunicator;
    private MessageDispatcher messageDispatcher;
    private final CommandParser commandParser;
    private final Map<String, ConsolidatedWatchdogConfig> ecWatchdogMap;

    public PlatformControlHandler(
            CommandParser commandParser,
            Map<String, ConsolidatedWatchdogConfig> ecWatchdogMap
    ) {
        this.commandParser = commandParser;
        this.ecWatchdogMap = ecWatchdogMap;

        log.info(() ->
                "[PlatformControlHandler][constructor] Platform control handler initialized"
                        + " | configuredECs=" + ecWatchdogMap.size()
        );
    }

    public void setControlChannelCommunicator(ExternalChanelCommunicator externalChanelCommunicator) {
        this.externalChanelCommunicator = externalChanelCommunicator;

        log.debug(() ->
                "[PlatformControlHandler][setControlChannelCommunicator] Control channel communicator configured"
                        + " | communicatorType="
                        + (externalChanelCommunicator == null
                        ? "null"
                        : externalChanelCommunicator.getClass().getSimpleName())
        );
    }

    public void setMessageDispatcher(MessageDispatcher messageDispatcher) {
        this.messageDispatcher = messageDispatcher;

        log.debug(() ->
                "[PlatformControlHandler][setMessageDispatcher] Message dispatcher configured"
                        + " | dispatcherType="
                        + (messageDispatcher == null
                        ? "null"
                        : messageDispatcher.getClass().getSimpleName())
        );
    }

    private ConsolidatedWatchdogConfig getWatchdog(String ecName) {

        ConsolidatedWatchdogConfig watchdog = ecWatchdogMap.get(ecName);

        log.debug(() ->
                "[PlatformControlHandler][getWatchdog] WATCHDOG configuration lookup completed"
                        + " | ecName=" + ecName
                        + " | watchdogFound=" + (watchdog != null)
        );

        return watchdog;
    }

    private Throwable unwrapCompletionError(Throwable error) {

        while (
                (error instanceof CompletionException
                        || error instanceof ExecutionException)
                        && error.getCause() != null
        ) {
            error = error.getCause();
        }

        return error;
    }

    private Message dispatchECOperation(
            Message command,
            String ecName,
            Operation operation,
            String acknowledgementText,
            String action
    ) {

        log.info(() ->
                "[PlatformControlHandler][dispatchECOperation] Initiating EC lifecycle operation"
                        + " | action=" + action
                        + " | ecName=" + ecName
                        + " | operation=" + operation
                        + " | correlationId=" + command.getCorrelationId()
        );

        Message acknowledgement = commandParser.createCommandResponse(
                command,
                acknowledgementText
        );

        trace(
                "CREATED ACK",
                acknowledgement
        );

        acknowledgement.setCorrelationId(command.getCorrelationId());
        acknowledgement.setResponseComplete(false);

        ConsolidatedWatchdogConfig watchdog = getWatchdog(ecName);

        Message controlRequest = createECControlRequest(
                command,
                ecName,
                operation,
                watchdog
        );

        trace(
                "CREATED EC CONTROL REQUEST",
                controlRequest
        );

        CompletableFuture<Message> watchdogFuture =
                new CompletableFuture<>();

        FutureReplyChannel watchdogReplyChannel =
                createWatchdogReplyChannel(
                        command,
                        watchdogFuture
                );

        completeWatchdogOperation(
                command,
                watchdogFuture,
                action
        );

        trace(
                "SENT WATCHDOG REQUEST",
                command
        );

        messageDispatcher.dispatchResponse(acknowledgement);

        trace(
                "SENT MESSAGE RESPONSE",
                command
        );

        try {

            log.debug(() ->
                    "[PlatformControlHandler][dispatchECOperation] Dispatching lifecycle request to WATCHDOG"
                            + " | action=" + action
                            + " | ecName=" + ecName
                            + " | watchdogId=" + watchdog.id()
                            + " | correlationId=" + command.getCorrelationId()
            );

            externalChanelCommunicator.sendRequest(
                    controlRequest,
                    watchdogReplyChannel
            ).whenComplete((ignored, sendError) -> {

                if (sendError != null) {

                    Throwable cause = unwrapCompletionError(sendError);

                    log.error(
                            "[PlatformControlHandler][dispatchECOperation] Unable to dispatch WATCHDOG control request"
                                    + " | action=" + action
                                    + " | ecName=" + ecName
                                    + " | correlationId=" + command.getCorrelationId(),
                            cause
                    );

                    watchdogFuture.completeExceptionally(cause);

                } else {

                    log.debug(() ->
                            "[PlatformControlHandler][dispatchECOperation] WATCHDOG control request submitted successfully"
                                    + " | action=" + action
                                    + " | ecName=" + ecName
                                    + " | correlationId=" + command.getCorrelationId()
                    );
                }
            });

        } catch (Exception e) {

            log.error(
                    "[PlatformControlHandler][dispatchECOperation] Exception while submitting WATCHDOG control request"
                            + " | action=" + action
                            + " | ecName=" + ecName,
                    e
            );

            watchdogFuture.completeExceptionally(e);
        }

        return null;
    }

    private Message createECControlRequest(
            Message command,
            String ecName,
            Operation operation,
            ConsolidatedWatchdogConfig watchdog
    ) {

        log.debug(() ->
                "[PlatformControlHandler][createECControlRequest] Creating EC control request"
                        + " | ecName=" + ecName
                        + " | operation=" + operation
                        + " | correlationId=" + command.getCorrelationId()
        );

        Message watchdogRequest = commandParser.createEmptyMessage();

        watchdogRequest.setOperation(operation);
        watchdogRequest.setMessageType(MessageType.REQUEST);
        watchdogRequest.setRecipientID(watchdog.id());
        watchdogRequest.setCorrelationId(command.getCorrelationId());

        Message controlRequest = createControlMessage(
                watchdogRequest,
                ecName
        );

        controlRequest.setCorrelationId(command.getCorrelationId());

        log.debug(() ->
                "[PlatformControlHandler][createECControlRequest] EC control request created"
                        + " | ecName=" + ecName
                        + " | watchdogId=" + watchdog.id()
                        + " | operation=" + operation
                        + " | correlationId=" + controlRequest.getCorrelationId()
        );

        return controlRequest;
    }

    private FutureReplyChannel createWatchdogReplyChannel(
            Message command,
            CompletableFuture<Message> watchdogFuture
    ) {

        log.debug(() ->
                "[PlatformControlHandler][createWatchdogReplyChannel] Creating WATCHDOG reply channel"
                        + " | correlationId=" + command.getCorrelationId()
        );

        return new FutureReplyChannel(
                watchdogFuture,

                progress -> {
                    try {
                        dispatchWatchdogProgress(command, progress);

                        return CompletableFuture.completedFuture(null);

                    } catch (Exception e) {

                        log.error(
                                "[PlatformControlHandler][createWatchdogReplyChannel] Progress dispatch failed"
                                        + " | correlationId=" + command.getCorrelationId(),
                                e
                        );

                        return CompletableFuture.failedFuture(e);
                    }
                }
        );
    }

    private void dispatchWatchdogProgress(
            Message command,
            Message progress
    ) {

        log.debug(() ->
                "[PlatformControlHandler][dispatchWatchdogProgress] WATCHDOG progress received"
                        + " | correlationId=" + command.getCorrelationId()
        );

        try {

            String status = commandParser.fromPayload(progress.getPayload(), org.prats.definitions.records.ControlResponse.class).status();

            log.debug(() ->
                    "[PlatformControlHandler][dispatchWatchdogProgress] WATCHDOG progress status extracted"
                            + " | ecCorrelationId=" + command.getCorrelationId()
                            + " | status=" + status
            );

            Message progressResponse = commandParser.createCommandResponse(
                    command,
                    status
            );

            progressResponse.setCorrelationId(command.getCorrelationId());
            progressResponse.setResponseComplete(false);

            messageDispatcher.dispatchResponse(progressResponse);

            log.debug(() ->
                    "[PlatformControlHandler][dispatchWatchdogProgress] Progress response dispatched to original command caller"
                            + " | correlationId=" + command.getCorrelationId()
                            + " | status=" + status
            );

        } catch (JsonProcessingException e) {

            log.error(
                    "[PlatformControlHandler][dispatchWatchdogProgress] Unable to parse WATCHDOG progress",
                    e
            );

            Message errorResponse = commandParser.createErrorResponse(
                    command.getCorrelationId(), "WATCHDOG_PROGRESS_INVALID",
                    "Unable to parse WATCHDOG progress update."
            );

            errorResponse.setCorrelationId(command.getCorrelationId());
            errorResponse.setResponseComplete(false);

            messageDispatcher.dispatchResponse(errorResponse);
        }
    }

    String watchdogResponseText(Message response) throws JsonProcessingException {
        if (response.hasError()) return response.getError().getErrorMessage();
        return commandParser.fromPayload(response.getPayload(),
                org.prats.definitions.records.ControlResponse.class).status();
    }

    private void completeWatchdogOperation(
            Message command,
            CompletableFuture<Message> watchdogFuture,
            String action
    ) {

        trace(
                "STEPPED IN completeWatchdogOperation",
                command
        );

        log.debug(() ->
                "[PlatformControlHandler][completeWatchdogOperation] WATCHDOG completion handler registered"
                        + " | action=" + action
                        + " | correlationId=" + command.getCorrelationId()
        );

        watchdogFuture.whenComplete((response, error) -> {

            Message finalResponse;

            if (error != null) {

                Throwable cause = unwrapCompletionError(error);

                String errorMessage = cause == null
                        ? "Unknown asynchronous failure."
                        : cause.getMessage() != null
                        ? cause.getMessage()
                        : cause.getClass().getSimpleName();

                log.error(
                        "[PlatformControlHandler][completeWatchdogOperation] WATCHDOG operation failed"
                                + " | action=" + action
                                + " | correlationId=" + command.getCorrelationId(),
                        cause == null ? error : cause
                );

                finalResponse = commandParser.createErrorResponse(
                        command.getCorrelationId(), "WATCHDOG_FAILED",
                        "Failed to " + action + " EC: " + errorMessage
                );

            } else {

                try {

                    if (response == null) {

                        throw new IllegalStateException(
                                "WATCHDOG completed the operation "
                                        + "without returning a response."
                        );
                    }

                    String status = watchdogResponseText(response);

                    if (status == null || status.isBlank()) {

                        throw new IllegalStateException(
                                "WATCHDOG response contains an empty status field."
                        );
                    }

                    log.info(() ->
                            "[PlatformControlHandler][completeWatchdogOperation] WATCHDOG operation completed"
                                    + " | action=" + action
                                    + " | correlationId=" + command.getCorrelationId()
                                    + " | status=" + status
                    );

                    finalResponse = response.hasError()
                            ? commandParser.createErrorResponse(command.getCorrelationId(), response.getError().getErrorCode(), status)
                            : commandParser.createCommandResponse(command, status);

                } catch (JsonProcessingException | RuntimeException e) {

                    log.error(
                            "[PlatformControlHandler][completeWatchdogOperation] Unable to process WATCHDOG final response",
                            e
                    );

                    finalResponse = commandParser.createErrorResponse(
                            command.getCorrelationId(), "WATCHDOG_RESPONSE_INVALID",
                            "WATCHDOG final response could not be processed."
                    );
                }
            }

            final Message completedResponse = finalResponse;

            completedResponse.setCorrelationId(command.getCorrelationId());
            completedResponse.setResponseComplete(true);

            log.debug(() ->
                    "[PlatformControlHandler][completeWatchdogOperation] Dispatching final EC lifecycle response"
                            + " | action=" + action
                            + " | correlationId=" + command.getCorrelationId()
                            + " | responseComplete=" + completedResponse.isResponseComplete()
            );

            messageDispatcher.dispatchResponse(completedResponse);

        });
    }

    public Message handleECStart(Message command, String ecName) {

        trace(
                "STEPPED IN handleECStart",
                command
        );

        log.info(() ->
                "[PlatformControlHandler][handleECStart] EC start operation requested"
                        + " | ecName=" + ecName
                        + " | correlationId=" + command.getCorrelationId()
        );

        return dispatchECOperation(
                command,
                ecName,
                Operation.START,
                "Starting EC: " + ecName,
                "start"
        );
    }


    public Message handleECStop(Message command, String ecName) {

        log.info(() ->
                "[PlatformControlHandler][handleECStop] EC stop operation requested"
                        + " | ecName=" + ecName
                        + " | correlationId=" + command.getCorrelationId()
        );

        return dispatchECOperation(
                command,
                ecName,
                Operation.STOP,
                "Stopping EC: " + ecName,
                "stop"
        );
    }


    public Message handleECRestart(Message command, String ecName) {

        log.info(() ->
                "[PlatformControlHandler][handleECRestart] EC restart operation requested"
                        + " | ecName=" + ecName
                        + " | correlationId=" + command.getCorrelationId()
        );

        return dispatchECOperation(
                command,
                ecName,
                Operation.RESTART,
                "Restarting EC: " + ecName,
                "restart"
        );
    }

    private Message createControlMessage(
            Message controlMessage,
            String value
    ) {

        log.debug(() ->
                "[PlatformControlHandler][createControlMessage] Adding control payload field"
                        + " | value=" + value
        );

        controlMessage.setPayload(commandParser.toPayload(new org.prats.definitions.records.ControlRequest(value)));
        return controlMessage;
    }

    private void trace(String stage, Message message) {

        log.debug(() ->
                "[TRACE][PlatformControlHandler][" + stage + "]"
                        + " | correlationId="
                        + (message == null ? "null" : message.getCorrelationId())
                        + " | thread="
                        + Thread.currentThread().getName()
        );
    }
}
