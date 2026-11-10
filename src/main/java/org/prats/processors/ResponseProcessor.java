package org.prats.processors;

import org.prats.definitions.Message;
import org.prats.definitions.MessageType;
import org.prats.definitions.PendingDelivery;
import org.prats.loggers.ShellLogger;
import org.prats.parsers.MessageParser;
import org.prats.registries.CorrelationRegistry;
import org.prats.registries.RetryRegistry;
import org.prats.replyChannels.ReplyChannel;

import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class ResponseProcessor {

    private final CorrelationRegistry correlationRegistry;
    private final RetryRegistry retryRegistry;
    private final MessageParser messageParser;

    public ResponseProcessor(
            MessageParser messageParser,
            CorrelationRegistry correlationRegistry,
            RetryRegistry retryRegistry
    ) {
        this.messageParser          = messageParser;
        this.correlationRegistry    = correlationRegistry;
        this.retryRegistry          = retryRegistry;
    }

    public void process(Message message, ReplyChannel replyChannel) {
        attemptDelivery(
                message, replyChannel
        );
    }

    // for first time deliveries
    public void process(Message response) {
        UUID correlationId = response.getCorrelationId();
        ReplyChannel replyChannel =
                correlationRegistry.removeAndGet(correlationId);
        if (replyChannel == null) {
            ShellLogger.log(
                    "No pending request found for correlation ID: "
                            + correlationId
            );
            return;
        }
        attemptDelivery(response, replyChannel);
    }

    // attempt to deliver on the reply channel
    private void attemptDelivery(
            Message response,
            ReplyChannel replyChannel
    ) {
        try {
            CompletionStage<Void> delivery =
                    replyChannel.sendReply(response);
            if (delivery == null) {
                throw new IllegalStateException(
                        "ReplyChannel returned a null CompletionStage"
                );
            }
            delivery.whenComplete((result, exception) -> {
                if (exception == null) {
                    ShellLogger.log(
                            "Response delivered successfully: "
                                    + response.getCorrelationId()
                    );
                } else {
                    handleInitialDeliveryFailure(
                            response,
                            replyChannel,
                            unwrap(exception)
                    );
                }
            });
        } catch (Exception exception) {
            handleInitialDeliveryFailure(
                    response,
                    replyChannel,
                    exception
            );
        }
    }

    // in case of delivery failure
    private void handleInitialDeliveryFailure(
            Message response,
            ReplyChannel replyChannel,
            Throwable exception
    ) {
        UUID correlationId = response.getCorrelationId();
        ShellLogger.log(
                "Initial response delivery failed for "
                        + correlationId + ": "
                        + exception.getMessage()
        );
        PendingDelivery pendingDelivery = new PendingDelivery(
                correlationId,
                response,
                replyChannel
        );
        if (!retryRegistry.add(pendingDelivery)) {
            ShellLogger.log(
                    "Unable to register response for retry: "
                            + correlationId
            );
        }
    }

    public void retry(PendingDelivery pendingDelivery) {

        UUID correlationId = pendingDelivery.getCorrelationId();

        if (!pendingDelivery.tryStartRetry()) {
            return;
        }

        try {
            if (retryRegistry.get(correlationId) != pendingDelivery) {
                return;
            }
            CompletionStage<Void> delivery =
                    pendingDelivery.getReplyChannel()
                            .sendReply(pendingDelivery.getResponse());
            if (delivery == null) {
                throw new IllegalStateException(
                        "ReplyChannel returned a null CompletionStage"
                );
            }

            delivery.whenComplete((result, exception) -> {
                try {
                    if (exception == null) {
                        retryRegistry.remove(
                                correlationId,
                                pendingDelivery
                        );
                        ShellLogger.log(
                                "Response retry delivered successfully: "
                                        + correlationId
                        );
                    } else {
                        handleRetryFailure(
                                pendingDelivery,
                                unwrap(exception)
                        );
                    }
                } finally {
                    pendingDelivery.finishRetry();
                }
            });
        } catch (Exception exception) {
            try {
                handleRetryFailure(
                        pendingDelivery,
                        exception
                );
            } finally {
                pendingDelivery.finishRetry();
            }
        }
    }
    // failures of retires
    private void handleRetryFailure(
            PendingDelivery pendingDelivery,
            Throwable exception
    ) {

        UUID correlationId = pendingDelivery.getCorrelationId();
        ShellLogger.log(
                "Response retry failed for "
                        + correlationId + ": "
                        + exception.getMessage()
        );
        boolean retryExhausted =
                retryRegistry.registerRetryFailure(correlationId);
        if (retryExhausted) {
            ShellLogger.log(
                    "Maximum retry limit ("
                            + retryRegistry.getMaxRetries()
                            + ") exhausted for correlation ID: "
                            + correlationId
            );
        }
    }

    private Throwable unwrap(Throwable exception) {

        while ((exception instanceof CompletionException
                || exception instanceof ExecutionException)
                && exception.getCause() != null) {

            exception = exception.getCause();
        }

        return exception;
    }

    public void processExpiry(UUID correlationId, ReplyChannel replyChannel) {

        Message errorMessage = messageParser.createErrorMessage(
                correlationId,
                "TIMEOUT",
                "There was an internal timeout while processing this request with correlation ID: " + correlationId
        );
        replyChannel.sendReply(
                errorMessage
        );
    }
}