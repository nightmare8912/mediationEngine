package org.prats.registries;

import org.prats.refractoring.PendingResponse;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class RetryRegistry {

    private final ConcurrentHashMap<UUID, PendingResponse> pendingResponses;
    private final int maxRetries;

    public RetryRegistry(int maxRetries) {

        if (maxRetries < 0) {
            throw new IllegalArgumentException(
                    "Maximum retries cannot be negative"
            );
        }

        this.maxRetries         = maxRetries;
        this.pendingResponses   = new ConcurrentHashMap<>();
    }

    public void add(PendingResponse pendingResponse) {

        if (!canRetry(pendingResponse)) {
            return;
        }

        pendingResponses.putIfAbsent(
                pendingResponse.getCorrelationId(),
                pendingResponse
        );
    }

    public PendingResponse get(UUID correlationId) {
        return pendingResponses.get(correlationId);
    }

    public PendingResponse removeAndGet(UUID correlationId) {
        return pendingResponses.remove(correlationId);
    }

    public boolean canRetry(PendingResponse pendingResponse) {

        synchronized (pendingResponse) {
            return pendingResponse.getRetryCount() < maxRetries;
        }
    }

    public boolean registerRetryFailure(UUID correlationId) {

        PendingResponse pendingResponse = pendingResponses.get(correlationId);

        if (pendingResponse == null) {
            return false;
        }

        synchronized (pendingResponse) {

            if (pendingResponses.get(correlationId) != pendingResponse) {
                return false;
            }

            int retryCount = pendingResponse.incrementRetryCount();

            if (retryCount >= maxRetries) {
                pendingResponses.remove(correlationId, pendingResponse);
                return true;
            }

            return false;
        }
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public List<PendingResponse> getAllFailures() {
        return List.copyOf(pendingResponses.values());
    }
}