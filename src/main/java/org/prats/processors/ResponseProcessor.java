
package org.prats.processors;

import org.prats.definitions.classes.Message;
import org.prats.definitions.classes.PendingDelivery;
import org.prats.loggers.FileLogger;
import org.prats.parsers.MessageParser;
import org.prats.registries.CorrelationRegistry;
import org.prats.registries.RetryRegistry;
import org.prats.definitions.interfaces.ReplyChannel;

import java.util.Objects;
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

        this.messageParser = Objects.requireNonNull(messageParser);
        this.correlationRegistry = Objects.requireNonNull(correlationRegistry);
        this.retryRegistry = Objects.requireNonNull(retryRegistry);
    }

    public void process(
            Message message,
            ReplyChannel replyChannel
    ) {
        attemptDelivery(message, replyChannel);
    }


    public void process(Message response) {

        UUID correlationId = response.getCorrelationId();

        ReplyChannel replyChannel;

        if (response.isResponseComplete()) {
            replyChannel =
                    correlationRegistry.removeAndGet(correlationId);
        } else {
            replyChannel =
                    correlationRegistry.get(correlationId);
            correlationRegistry.resetTimeout(
                    correlationId
            );
        }

        if (replyChannel == null) {

            FileLogger.log(
                    "No pending request found for correlation ID: "
                            + correlationId
            );

            return;
        }

        attemptDelivery(response, replyChannel);
    }


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

                    FileLogger.log(
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

    private void handleInitialDeliveryFailure(
            Message response,
            ReplyChannel replyChannel,
            Throwable exception
    ) {

        UUID correlationId = response.getCorrelationId();

        FileLogger.log(
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

            FileLogger.log(
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


        if (retryRegistry.get(correlationId) != pendingDelivery) {

            pendingDelivery.finishRetry();

            return;
        }


        if (retryRegistry.isExpired(pendingDelivery)) {

            retryRegistry.remove(
                    correlationId,
                    pendingDelivery
            );

            FileLogger.log(
                    "Pending delivery TTL expired for correlation ID: "
                            + correlationId
            );

            pendingDelivery.finishRetry();

            return;
        }

        if (!retryRegistry.canRetry(pendingDelivery)) {

            retryRegistry.remove(
                    correlationId,
                    pendingDelivery
            );

            pendingDelivery.finishRetry();

            return;
        }

        CompletionStage<Void> delivery;

        try {

            delivery = pendingDelivery.getReplyChannel()
                    .sendReply(pendingDelivery.getResponse());

            if (delivery == null) {

                throw new IllegalStateException(
                        "ReplyChannel returned a null CompletionStage"
                );
            }

        } catch (Exception exception) {

            try {

                handleRetryFailure(
                        pendingDelivery,
                        exception
                );

            } finally {

                pendingDelivery.finishRetry();
            }

            return;
        }


        try {

            delivery.whenComplete((result, exception) -> {

                try {

                    if (exception == null) {

                        retryRegistry.remove(
                                correlationId,
                                pendingDelivery
                        );

                        FileLogger.log(
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


    private void handleRetryFailure(
            PendingDelivery pendingDelivery,
            Throwable exception
    ) {

        UUID correlationId = pendingDelivery.getCorrelationId();

        FileLogger.log(
                "Response retry failed for "
                        + correlationId + ": "
                        + exception.getMessage()
        );

        if (retryRegistry.isExpired(pendingDelivery)) {

            retryRegistry.remove(
                    correlationId,
                    pendingDelivery
            );

            FileLogger.log(
                    "Pending delivery TTL expired for correlation ID: "
                            + correlationId
            );

            return;
        }

        boolean retryExhausted =
                retryRegistry.registerRetryFailure(pendingDelivery);

        if (retryExhausted) {

            FileLogger.log(
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


    public void processExpiry(
            UUID correlationId,
            ReplyChannel replyChannel
    ) {

        Message errorMessage = messageParser.createErrorResponse(
                correlationId,
                "TIMEOUT",
                "There was an internal timeout while processing this request "
                        + "with correlation ID: " + correlationId
        );

        try {

            CompletionStage<Void> delivery =
                    replyChannel.sendReply(errorMessage);

            if (delivery == null) {

                throw new IllegalStateException(
                        "ReplyChannel returned a null CompletionStage"
                );
            }

            delivery.whenComplete((result, exception) -> {

                if (exception == null) {

                    FileLogger.log(
                            "Timeout response delivered: "
                                    + correlationId
                    );

                } else {

                    FileLogger.log(
                            "Timeout response delivery failed for "
                                    + correlationId + ": "
                                    + unwrap(exception).getMessage()
                    );
                }
            });

        } catch (Exception exception) {

            FileLogger.log(
                    "Unable to send timeout response for "
                            + correlationId + ": "
                            + exception.getMessage()
            );
        }
    }
}
