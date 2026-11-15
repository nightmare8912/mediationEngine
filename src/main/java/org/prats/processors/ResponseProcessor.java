package org.prats.processors;

import org.prats.definitions.classes.Message;
import org.prats.definitions.classes.PendingDelivery;
import org.prats.loggers.EngineLogger;
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

    private static final EngineLogger log =
            EngineLogger.getLogger(ResponseProcessor.class);

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

        log.debug(
                "[ResponseProcessor][constructor] Response processor initialized"
        );
    }

    public void process(
            Message message,
            ReplyChannel replyChannel
    ) {

        log.debug(() ->
                "[ResponseProcessor][process] Initiating response delivery"
                        + " | correlationId=" + message.getCorrelationId()
                        + " | responseComplete=" + message.isResponseComplete()
                        + " | replyChannel="
                        + replyChannel.getClass().getSimpleName()
        );

        attemptDelivery(message, replyChannel);
    }


    public void process(Message response) {

        UUID correlationId = response.getCorrelationId();

        log.debug(() ->
                "[ResponseProcessor][process] Processing response using correlation registry"
                        + " | correlationId=" + correlationId
                        + " | responseComplete=" + response.isResponseComplete()
        );

        ReplyChannel replyChannel;

        if (response.isResponseComplete()) {

            log.debug(() ->
                    "[ResponseProcessor][process] Final response received; removing correlation"
                            + " | correlationId=" + correlationId
            );

            replyChannel =
                    correlationRegistry.removeAndGet(correlationId);

        } else {

            log.debug(() ->
                    "[ResponseProcessor][process] Intermediate response received; retaining correlation"
                            + " | correlationId=" + correlationId
            );

            replyChannel =
                    correlationRegistry.get(correlationId);

            correlationRegistry.resetTimeout(
                    correlationId
            );
        }

        if (replyChannel == null) {

            log.warn(() ->
                    "[ResponseProcessor][process] No pending request found for correlation ID"
                            + " | correlationId=" + correlationId
            );

            return;
        }

        log.debug(() ->
                "[ResponseProcessor][process] Correlation resolved; forwarding response for delivery"
                        + " | correlationId=" + correlationId
        );

        attemptDelivery(response, replyChannel);
    }


    private void attemptDelivery(
            Message response,
            ReplyChannel replyChannel
    ) {

        UUID correlationId = response.getCorrelationId();

        log.debug(() ->
                "[ResponseProcessor][attemptDelivery] Starting initial response delivery"
                        + " | correlationId=" + correlationId
                        + " | replyChannel="
                        + replyChannel.getClass().getSimpleName()
        );

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

                    log.debug(() ->
                            "[ResponseProcessor][attemptDelivery] Response delivered successfully"
                                    + " | correlationId=" + correlationId
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

        log.error(
                "[ResponseProcessor][handleInitialDeliveryFailure] Initial response delivery failed"
                        + " | correlationId=" + correlationId,
                exception
        );

        PendingDelivery pendingDelivery = new PendingDelivery(
                correlationId,
                response,
                replyChannel
        );

        if (!retryRegistry.add(pendingDelivery)) {

            log.warn(() ->
                    "[ResponseProcessor][handleInitialDeliveryFailure] Unable to register response for retry"
                            + " | correlationId=" + correlationId
                            + " | retryCount=" + pendingDelivery.getRetryCount()
            );

        } else {

            log.debug(() ->
                    "[ResponseProcessor][handleInitialDeliveryFailure] Response registered for retry"
                            + " | correlationId=" + correlationId
                            + " | retryCount=" + pendingDelivery.getRetryCount()
            );
        }
    }


    public void retry(PendingDelivery pendingDelivery) {

        UUID correlationId = pendingDelivery.getCorrelationId();

        log.debug(() ->
                "[ResponseProcessor][retry] Processing pending response delivery"
                        + " | correlationId=" + correlationId
                        + " | retryCount=" + pendingDelivery.getRetryCount()
        );

        if (!pendingDelivery.tryStartRetry()) {

            log.debug(() ->
                    "[ResponseProcessor][retry] Retry skipped because another retry is already in progress"
                            + " | correlationId=" + correlationId
            );

            return;
        }


        if (retryRegistry.get(correlationId) != pendingDelivery) {

            log.debug(() ->
                    "[ResponseProcessor][retry] Retry skipped because pending delivery is no longer registered"
                            + " | correlationId=" + correlationId
            );

            pendingDelivery.finishRetry();

            return;
        }


        if (retryRegistry.isExpired(pendingDelivery)) {

            retryRegistry.remove(
                    correlationId,
                    pendingDelivery
            );

            log.warn(() ->
                    "[ResponseProcessor][retry] Pending delivery TTL expired"
                            + " | correlationId=" + correlationId
                            + " | retryCount=" + pendingDelivery.getRetryCount()
            );

            pendingDelivery.finishRetry();

            return;
        }


        if (!retryRegistry.canRetry(pendingDelivery)) {

            retryRegistry.remove(
                    correlationId,
                    pendingDelivery
            );

            log.warn(() ->
                    "[ResponseProcessor][retry] Pending delivery is no longer eligible for retry"
                            + " | correlationId=" + correlationId
                            + " | retryCount=" + pendingDelivery.getRetryCount()
                            + " | maxRetries=" + retryRegistry.getMaxRetries()
            );

            pendingDelivery.finishRetry();

            return;
        }

        log.debug(() ->
                "[ResponseProcessor][retry] Attempting response redelivery"
                        + " | correlationId=" + correlationId
                        + " | retryCount=" + pendingDelivery.getRetryCount()
        );

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

                        log.debug(() ->
                                "[ResponseProcessor][retry] Response retry delivered successfully"
                                        + " | correlationId=" + correlationId
                                        + " | retryCount=" + pendingDelivery.getRetryCount()
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

        log.error(
                "[ResponseProcessor][handleRetryFailure] Response retry failed"
                        + " | correlationId=" + correlationId
                        + " | retryCount=" + pendingDelivery.getRetryCount(),
                exception
        );

        if (retryRegistry.isExpired(pendingDelivery)) {

            retryRegistry.remove(
                    correlationId,
                    pendingDelivery
            );

            log.warn(() ->
                    "[ResponseProcessor][handleRetryFailure] Pending delivery TTL expired; delivery removed"
                            + " | correlationId=" + correlationId
                            + " | retryCount=" + pendingDelivery.getRetryCount()
            );

            return;
        }

        boolean retryExhausted =
                retryRegistry.registerRetryFailure(pendingDelivery);

        if (retryExhausted) {

            log.warn(() ->
                    "[ResponseProcessor][handleRetryFailure] Maximum retry limit exhausted"
                            + " | correlationId=" + correlationId
                            + " | maxRetries=" + retryRegistry.getMaxRetries()
                            + " | retryCount=" + pendingDelivery.getRetryCount()
            );

        } else {

            log.debug(() ->
                    "[ResponseProcessor][handleRetryFailure] Retry failure registered; delivery remains eligible for retry"
                            + " | correlationId=" + correlationId
                            + " | retryCount=" + pendingDelivery.getRetryCount()
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

        log.warn(() ->
                "[ResponseProcessor][processExpiry] Processing request timeout"
                        + " | correlationId=" + correlationId
        );

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

                    log.debug(() ->
                            "[ResponseProcessor][processExpiry] Timeout response delivered successfully"
                                    + " | correlationId=" + correlationId
                    );

                } else {

                    Throwable deliveryFailure = unwrap(exception);

                    log.error(
                            "[ResponseProcessor][processExpiry] Timeout response delivery failed"
                                    + " | correlationId=" + correlationId,
                            deliveryFailure
                    );
                }
            });

        } catch (Exception exception) {

            log.error(
                    "[ResponseProcessor][processExpiry] Unable to send timeout response"
                            + " | correlationId=" + correlationId,
                    exception
            );
        }
    }
}