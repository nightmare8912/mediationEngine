package org.prats.replyChannels;

import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.ReplyChannel;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class FutureReplyChannel implements ReplyChannel {

    private final CompletableFuture<Message> future;

    private final Function<Message, CompletionStage<Void>> progressHandler;

    public FutureReplyChannel(
            CompletableFuture<Message> future,
            Function<Message, CompletionStage<Void>> progressHandler
    ) {
        this.future = Objects.requireNonNull(future);
        this.progressHandler = Objects.requireNonNull(progressHandler);
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
            try {
                return Objects.requireNonNull(
                        progressHandler.apply(response)
                );
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        if (!future.complete(response)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "Future has already been completed"
                    )
            );
        }

        return CompletableFuture.completedFuture(null);
    }
}