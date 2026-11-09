package org.prats.refractoring;

import org.prats.definitions.Message;
import org.prats.replyChannels.ReplyChannel;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class PendingResponse {

    private final UUID correlationId;
    private final Message response;
    private final ReplyChannel replyChannel;
    private final AtomicBoolean retryInProgress;

    private int retryCount;

    public PendingResponse(
            UUID correlationId,
            Message response,
            ReplyChannel replyChannel
    ) {
        this.correlationId          = correlationId;
        this.response               = response;
        this.replyChannel           = replyChannel;
        this.retryCount             = 0;
        this.retryInProgress        = new AtomicBoolean(false);
    }

    public UUID getCorrelationId() {
        return correlationId;
    }

    public Message getResponse() {
        return response;
    }

    public ReplyChannel getReplyChannel() {
        return replyChannel;
    }

    public int incrementRetryCount() {
        return ++retryCount;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public boolean tryStartRetry() {
        return retryInProgress.compareAndSet(false, true);
    }

    public void finishRetry() {
        retryInProgress.set(false);
    }
}
