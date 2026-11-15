package org.prats.processors;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.interfaces.ReplyChannel;
import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;
import org.prats.registries.CorrelationRegistry;

import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

public class RequestProcessor {

    private static final EngineLogger log =
            EngineLogger.getLogger(RequestProcessor.class);

    private final CorrelationRegistry correlationRegistry;
    private final MessageParser messageParser;
    private final CommandProcessor commandProcessor;
    private final MessageProcessor messageProcessor;

    public RequestProcessor(
            CorrelationRegistry correlationRegistry,
            MessageParser messageParser,
            CommandProcessor commandProcessor,
            MessageProcessor messageProcessor
    ) {
        this.correlationRegistry = correlationRegistry;
        this.messageParser = messageParser;
        this.commandProcessor = commandProcessor;
        this.messageProcessor = messageProcessor;

        log.debug(
                "[RequestProcessor][constructor] Request processor initialized"
        );
    }

    public void process(
            Message requestMessage,
            ReplyChannel replyChannel
    ) {

        log.debug(() ->
                "[RequestProcessor][process] Incoming message received"
                        + " | operation=" + requestMessage.getOperation()
                        + " | messageType=" + requestMessage.getMessageType()
                        + " | correlationId=" + requestMessage.getCorrelationId()
        );

        /*
         * If both the operation and message type are UNKNOWN,
         * treat the incoming message as an unparsed command.
         *
         * Forward it to the CommandProcessor for interpretation.
         */
        if (requestMessage.getOperation() == Operation.UNKNOWN
                && requestMessage.getMessageType() == MessageType.UNKNOWN) {

            log.debug(() ->
                    "[RequestProcessor][process] Unparsed command detected"
                            + " | correlationId=" + requestMessage.getCorrelationId()
            );

            log.debug(() ->
                    "[RequestProcessor][process] Forwarding command to CommandProcessor"
                            + " | correlationId=" + requestMessage.getCorrelationId()
            );

            commandProcessor.process(
                    requestMessage,
                    replyChannel
            );

            log.debug(() ->
                    "[RequestProcessor][process] CommandProcessor invocation completed"
                            + " | correlationId=" + requestMessage.getCorrelationId()
            );

            return;
        }

        /*
         * The message has been parsed into the internal structure.
         *
         * If this is a REQUEST, register its correlation ID
         * and reply channel before dispatching it.
         */
        if (requestMessage.getMessageType() == MessageType.REQUEST) {

            log.debug(() ->
                    "[RequestProcessor][process] Registering request correlation"
                            + " | correlationId=" + requestMessage.getCorrelationId()
                            + " | currentCorrelations=" + correlationRegistry.getSize()
            );

            try {

                correlationRegistry.add(
                        requestMessage.getCorrelationId(),
                        replyChannel
                );

            } catch (RejectedExecutionException | IllegalStateException exception) {

                log.error(
                        "[RequestProcessor][process] Correlation registration rejected"
                                + " | correlationId=" + requestMessage.getCorrelationId()
                                + " | operation=" + requestMessage.getOperation()
                                + " | exception=" + exception.getClass().getSimpleName()
                                + " | reason=" + exception.getMessage()
                );

                Message overload = messageParser.createErrorResponse(
                        requestMessage.getCorrelationId(),
                        "OVERLOADED", "Too many in-flight requests. Retry after load drops."
                );

                overload.setOperation(
                        requestMessage.getOperation()
                );

                overload.setStatus("FAILED");

                log.debug(() ->
                        "[RequestProcessor][process] Sending overload response"
                                + " | correlationId=" + requestMessage.getCorrelationId()
                                + " | errorCode=OVERLOADED"
                );

                replyChannel.sendReply(overload);

                log.debug(() ->
                        "[RequestProcessor][process] Overload response submitted to reply channel"
                                + " | correlationId=" + requestMessage.getCorrelationId()
                );

                return;
            }

            log.debug(() ->
                    "[RequestProcessor][process] Request correlation registered successfully"
                            + " | correlationId=" + requestMessage.getCorrelationId()
                            + " | currentCorrelations=" + correlationRegistry.getSize()
            );

        } else {

            log.debug(() ->
                    "[RequestProcessor][process] Skipping correlation registration"
                            + " | messageType=" + requestMessage.getMessageType()
                            + " | correlationId=" + requestMessage.getCorrelationId()
            );
        }

        /*
         * Forward the parsed message to the MessageProcessor.
         *
         * If processing is rejected, remove the correlation
         * registered for this request and send an error response.
         */
        log.debug(() ->
                "[RequestProcessor][process] Dispatching message to MessageProcessor"
                        + " | operation=" + requestMessage.getOperation()
                        + " | messageType=" + requestMessage.getMessageType()
                        + " | correlationId=" + requestMessage.getCorrelationId()
        );

        try {

            messageProcessor.process(requestMessage);

            log.debug(() ->
                    "[RequestProcessor][process] MessageProcessor invocation completed successfully"
                            + " | operation=" + requestMessage.getOperation()
                            + " | correlationId=" + requestMessage.getCorrelationId()
            );

        } catch (RejectedExecutionException
                 | IllegalArgumentException
                 | IllegalStateException exception) {

            log.error(
                    "[RequestProcessor][process] Message processing failed"
                            + " | operation=" + requestMessage.getOperation()
                            + " | messageType=" + requestMessage.getMessageType()
                            + " | correlationId=" + requestMessage.getCorrelationId()
                            + " | exception=" + exception.getClass().getSimpleName()
                            + " | reason=" + exception.getMessage()
            );

            /*
             * Only REQUEST messages were registered here.
             * Avoid removing correlations for other message types.
             */
            if (requestMessage.getMessageType() == MessageType.REQUEST) {

                log.debug(() ->
                        "[RequestProcessor][process] Removing correlation after dispatch failure"
                                + " | correlationId=" + requestMessage.getCorrelationId()
                                + " | currentCorrelations=" + correlationRegistry.getSize()
                );

                correlationRegistry.removeAndGet(
                        requestMessage.getCorrelationId()
                );

                log.debug(() ->
                        "[RequestProcessor][process] Correlation cleanup completed"
                                + " | correlationId=" + requestMessage.getCorrelationId()
                                + " | remainingCorrelations=" + correlationRegistry.getSize()
                );
            }

            boolean overloaded =
                    exception instanceof RejectedExecutionException;

            String errorCode = overloaded
                    ? "OVERLOADED"
                    : "REQUEST_FAILED";

            String errorBody = overloaded
                    ? "Service overloaded. Retry after load drops."
                    : "Request could not be processed.";

            Message error = messageParser.createErrorResponse(
                    requestMessage.getCorrelationId(), errorCode, errorBody
            );

            error.setOperation(
                    requestMessage.getOperation()
            );

            error.setStatus("FAILED");

            log.debug(() ->
                    "[RequestProcessor][process] Sending failure response"
                            + " | correlationId=" + requestMessage.getCorrelationId()
                            + " | operation=" + requestMessage.getOperation()
                            + " | errorCode=" + errorCode
            );

            replyChannel.sendReply(error);

            log.debug(() ->
                    "[RequestProcessor][process] Failure response submitted to reply channel"
                            + " | correlationId=" + requestMessage.getCorrelationId()
                            + " | errorCode=" + errorCode
            );

            return;
        }

        log.debug(() ->
                "[RequestProcessor][process] Request processing completed"
                        + " | operation=" + requestMessage.getOperation()
                        + " | messageType=" + requestMessage.getMessageType()
                        + " | correlationId=" + requestMessage.getCorrelationId()
        );
    }
}