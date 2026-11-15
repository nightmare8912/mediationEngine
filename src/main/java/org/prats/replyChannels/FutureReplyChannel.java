package org.prats.replyChannels;

import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.ReplyChannel;
import org.prats.loggers.EngineLogger;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class FutureReplyChannel implements ReplyChannel {

    private static final EngineLogger log =
            EngineLogger.getLogger(FutureReplyChannel.class);

    private final CompletableFuture<Message> future;

    private final Function<Message, CompletionStage<Void>> progressHandler;

    public FutureReplyChannel(
            CompletableFuture<Message> future,
            Function<Message, CompletionStage<Void>> progressHandler
    ) {
        this.future = Objects.requireNonNull(future);
        this.progressHandler = Objects.requireNonNull(progressHandler);

        log.debug(() ->
                "[FutureReplyChannel][constructor] Reply channel initialized"
                        + " | futureDone=" + this.future.isDone()
                        + " | progressHandler="
                        + this.progressHandler.getClass().getName()
        );
    }

    public FutureReplyChannel(CompletableFuture<Message> future) {
        this(
                future,
                response -> CompletableFuture.completedFuture(null)
        );
    }

    @Override
    public CompletionStage<Void> sendReply(Message response) {

        Objects.requireNonNull(response);

        if (!response.isResponseComplete()) {

            log.debug(
                    "[FutureReplyChannel][sendReply] Progress response received; invoking progress handler"
            );

            try {
                CompletionStage<Void> progressStage = Objects.requireNonNull(
                        progressHandler.apply(response)
                );

                log.debug(
                        "[FutureReplyChannel][sendReply] Progress handler returned a completion stage"
                );

                return progressStage;

            } catch (RuntimeException exception) {

                log.error(
                        "[FutureReplyChannel][sendReply] Progress handler failed",
                        exception
                );

                return CompletableFuture.failedFuture(exception);
            }
        }

        log.debug(
                "[FutureReplyChannel][sendReply] Final response received; completing response future"
        );

        if (!future.complete(response)) {

            log.warn(
                    "[FutureReplyChannel][sendReply] Unable to complete response future because it was already completed"
            );

            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "Future has already been completed"
                    )
            );
        }

        log.debug(
                "[FutureReplyChannel][sendReply] Response future completed successfully"
        );

        return CompletableFuture.completedFuture(null);
    }
}