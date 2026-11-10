package org.prats.replyChannels;

import org.prats.definitions.Message;
import org.prats.definitions.RequestFailure;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class FutureReplyChannel implements ReplyChannel {

    private final CompletableFuture<Message> future;

    public FutureReplyChannel(CompletableFuture<Message> future) {
        this.future = Objects.requireNonNull(future);
    }

    @Override
    public CompletionStage<Void> sendReply(Message response) {
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