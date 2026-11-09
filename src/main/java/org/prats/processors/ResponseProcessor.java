package org.prats.processors;

import org.prats.definitions.Message;
import org.prats.definitions.Operation;
import org.prats.parsers.MessageParser;
import org.prats.loggers.ShellLogger;
import org.prats.refractoring.PendingResponse;
import org.prats.registries.CorrelationRegistry;
import org.prats.registries.RetryRegistry;
import org.prats.replyChannels.ReplyChannel;

import java.util.concurrent.CompletionStage;

public class ResponseProcessor {

    private final CorrelationRegistry correlationRegistry;
    private final MessageParser messageParser;
    private final RetryRegistry retryRegistry;

    public ResponseProcessor(
            CorrelationRegistry correlationRegistry,
            RetryRegistry retryRegistry,
            MessageParser messageParser
    ) {
        this.correlationRegistry    = correlationRegistry;
        this.retryRegistry          = retryRegistry;
        this.messageParser          = messageParser;
    }

    public void process(Message message) {
        // check if pending response is already created, which means a prev delivery was attempted
        PendingResponse pendingResponse =
                retryRegistry.get(message.getCorrelationId());

        // get the reply channel
        ReplyChannel replyChannel = pendingResponse != null
                ? pendingResponse.getReplyChannel()
                : correlationRegistry.get(message.getCorrelationId());

        process(message, replyChannel, pendingResponse);
    }

    public void process(Message message, ReplyChannel replyChannel) {

        PendingResponse pendingResponse =
                retryRegistry.get(message.getCorrelationId());

        if (pendingResponse != null) {
            replyChannel = pendingResponse.getReplyChannel();
        }

        process(message, replyChannel, pendingResponse);
    }

    private void process(
            Message message,
            ReplyChannel replyChannel,
            PendingResponse pendingResponse
    ) {

        // cannot respond if reply channel is null
        if (replyChannel == null) {
            ShellLogger.log(
                    "No ReplyChannel found for correlation ID: "
                            + message.getCorrelationId()
            );
            return;
        }

        try {

            String reply;
            // parse json replies for operations that didnt come from shell
            if (message.getOperation() != Operation.COMMAND) {
                reply = messageParser.createJSONFromMessage(message);
            } else {
                // for shell we pass non-json replies
                reply = messageParser.extractPayloadField(message, "body");
            }

            // send the reply and check if it is completed
            CompletionStage<Void> delivery =
                    replyChannel.sendReply(reply);

            if (delivery == null) {
                throw new IllegalStateException(
                        "ReplyChannel returned a null CompletionStage"
                );
            }

            // when delivery is complete, this runs, it can either be a success or an error
            delivery.whenComplete((result, exception) -> {
                if (exception == null) {
                    // if it is success we clear correlation
                    correlationRegistry.removeAndGet(
                            message.getCorrelationId()
                    );
                    // and the retry, as it may have something, we dont want duplicate delivery
                    retryRegistry.removeAndGet(
                            message.getCorrelationId()
                    );
                } else {
                    // case when delivery was not success
                    handleDeliveryFailure(
                            message,
                            replyChannel,
                            pendingResponse,
                            exception
                    );
                }
            });

        } catch (Exception exception) {
            // when we could not deliver
            handleDeliveryFailure(
                    message,
                    replyChannel,
                    pendingResponse,
                    exception
            );
        }
    }

    private void handleDeliveryFailure(
            Message message,
            ReplyChannel replyChannel,
            PendingResponse pendingResponse,
            Throwable exception
    ) {

        ShellLogger.log(
                "Failed to deliver response: " + exception.getMessage()
        );
        // cases where the first delivery was attempted and it failed so an entry has to be made in the retry registry
        if (pendingResponse == null) {

            PendingResponse newPendingResponse = new PendingResponse(
                    message.getCorrelationId(),
                    message,
                    replyChannel
            );
            // add to retry registry
            if (retryRegistry.canRetry(newPendingResponse)) {
                retryRegistry.add(newPendingResponse);

            } else {
                // if it cannot retry we remove it from out correlation registry
                correlationRegistry.removeAndGet(
                        message.getCorrelationId()
                );

                ShellLogger.log(
                        "No retries configured for correlation ID: "
                                + message.getCorrelationId()
                );
            }

        } else {
            // now this was not the first attempt, we check if we r eligible for more retries if not, the retry registry will remove the message from its memory
            boolean retryExhausted = retryRegistry.registerRetryFailure(
                    message.getCorrelationId()
            );
            // if retry is exhausted we remove from correlation registry
            if (retryExhausted) {
                correlationRegistry.removeAndGet(
                        message.getCorrelationId()
                );
                ShellLogger.log(
                        "Maximum retry limit ("
                                + retryRegistry.getMaxRetries()
                                + ") exhausted for correlation ID: "
                                + message.getCorrelationId()
                );
            }
        }
    }
}