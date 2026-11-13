
package org.prats.definitions.classes;

import org.prats.definitions.interfaces.ReplyChannel;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PendingDelivery {

    private final UUID correlationId;
    private final Message response;
    private final ReplyChannel replyChannel;

    private final AtomicBoolean retryInProgress;
    private final AtomicInteger retryCount;

    private final long createdAtNanos;

    public PendingDelivery(
            UUID correlationId,
            Message response,
            ReplyChannel replyChannel
    ) {
        this.correlationId = Objects.requireNonNull(
                correlationId,
                "Correlation ID cannot be null"
        );

        this.response = Objects.requireNonNull(
                response,
                "Response cannot be null"
        );

        this.replyChannel = Objects.requireNonNull(
                replyChannel,
                "Reply channel cannot be null"
        );

        this.retryCount = new AtomicInteger(0);
        this.retryInProgress = new AtomicBoolean(false);

        this.createdAtNanos = System.nanoTime();
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
        return retryCount.incrementAndGet();
    }

    public int getRetryCount() {
        return retryCount.get();
    }

    public boolean tryStartRetry() {
        return retryInProgress.compareAndSet(false, true);
    }

    public void finishRetry() {
        retryInProgress.set(false);
    }

    public boolean isExpired(long ttlNanos) {
        return System.nanoTime() - createdAtNanos >= ttlNanos;
    }
}
