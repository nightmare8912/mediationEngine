package org.prats.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.interfaces.ControlChannelCommunicator;
import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.definitions.records.ConsolidatedWatchdogConfig;
import org.prats.loggers.FileLogger;
import org.prats.parsers.CommandParser;
import org.prats.replyChannels.FutureReplyChannel;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public class PlatformControlHandler {

    private ControlChannelCommunicator controlChannelCommunicator;
    private MessageDispatcher messageDispatcher;
    private final CommandParser commandParser;
    private final Map<String, ConsolidatedWatchdogConfig> ecWatchdogMap;

    public PlatformControlHandler(
            CommandParser commandParser,
            Map<String, ConsolidatedWatchdogConfig> ecWatchdogMap
    ) {
        this.commandParser              = commandParser;
        this.ecWatchdogMap              = ecWatchdogMap;
    }

    public void setControlChannelCommunicator(ControlChannelCommunicator controlChannelCommunicator) {
        this.controlChannelCommunicator = controlChannelCommunicator;
    }

    public void setMessageDispatcher(MessageDispatcher messageDispatcher) {
        this.messageDispatcher      = messageDispatcher;
    }

    private ConsolidatedWatchdogConfig getWatchdog(String ecName) {
        return ecWatchdogMap.get(ecName);
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

            controlChannelCommunicator.sendRequest(
                    controlRequest,
                    watchdogReplyChannel
            ).whenComplete((ignored, sendError) -> {

                if (sendError != null) {

                    watchdogFuture.completeExceptionally(
                            unwrapCompletionError(sendError)
                    );
                }
            });

        } catch (Exception e) {

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

        Message watchdogRequest = commandParser.createEmptyMessage();

        watchdogRequest.setOperation(operation);
        watchdogRequest.setMessageType(MessageType.REQUEST);
        watchdogRequest.setRecipientID(watchdog.id());
        watchdogRequest.setCorrelationId(command.getCorrelationId());

        Message controlRequest = createControlMessage(
                watchdogRequest,
                "ecName",
                ecName
        );

        controlRequest.setCorrelationId(command.getCorrelationId());

        return controlRequest;
    }

    private FutureReplyChannel createWatchdogReplyChannel(
            Message command,
            CompletableFuture<Message> watchdogFuture
    ) {

        return new FutureReplyChannel(
                watchdogFuture,

                progress -> {
                    try {
                        dispatchWatchdogProgress(command, progress);

                        return CompletableFuture.completedFuture(null);

                    } catch (Exception e) {

                        FileLogger.log(
                                "[PlatformControlHandler] Progress dispatch failed: "
                                        + e.getMessage()
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

        try {

            String status = commandParser.extractPayloadField(
                    progress,
                    "status",
                    String.class
            );

            Message progressResponse = commandParser.createCommandResponse(
                    command,
                    status
            );

            progressResponse.setCorrelationId(command.getCorrelationId());
            progressResponse.setResponseComplete(false);

            messageDispatcher.dispatchResponse(progressResponse);

        } catch (JsonProcessingException e) {

            FileLogger.log(
                    "Unable to parse WATCHDOG progress: " + e.getMessage()
            );

            Message errorResponse = commandParser.createCommandResponse(
                    command,
                    "Unable to parse WATCHDOG progress update."
            );

            errorResponse.setCorrelationId(command.getCorrelationId());
            errorResponse.setResponseComplete(false);

            messageDispatcher.dispatchResponse(errorResponse);
        }
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

        watchdogFuture.whenComplete((response, error) -> {

            Message finalResponse;

            if (error != null) {

                Throwable cause = unwrapCompletionError(error);

                String errorMessage = cause.getMessage() != null
                        ? cause.getMessage()
                        : cause.getClass().getSimpleName();

                finalResponse = commandParser.createCommandResponse(
                        command,
                        "Failed to " + action + " EC: " + errorMessage
                );

            } else {

                try {

                    String status = commandParser.extractPayloadField(
                            response,
                            "status",
                            String.class
                    );

                    finalResponse = commandParser.createCommandResponse(
                            command,
                            status
                    );

                } catch (JsonProcessingException e) {

                    FileLogger.log(
                            "Unable to parse WATCHDOG final response: "
                                    + e.getMessage()
                    );

                    finalResponse = commandParser.createCommandResponse(
                            command,
                            "WATCHDOG final response could not be parsed."
                    );
                }
            }

            finalResponse.setCorrelationId(command.getCorrelationId());
            finalResponse.setResponseComplete(true);

            messageDispatcher.dispatchResponse(finalResponse);

        });
    }

    public Message handleECStart(Message command, String ecName) {

        trace(
                "STEPPED IN handleECStart",
                command
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

        return dispatchECOperation(
                command,
                ecName,
                Operation.STOP,
                "Stopping EC: " + ecName,
                "stop"
        );
    }


    public Message handleECRestart(Message command, String ecName) {

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
            String key,
            String value
    ) {
        return commandParser.putPayloadField(
                controlMessage,
                key,
                value
        );
    }

    private void trace(String stage, Message message) {

        FileLogger.log(
                "[TRACE][PlatformControlHandler][" + stage + "]"
                        + " | correlationId="
                        + (message == null ? "null" : message.getCorrelationId())
                        + " | thread="
                        + Thread.currentThread().getName()
        );
    }
}
