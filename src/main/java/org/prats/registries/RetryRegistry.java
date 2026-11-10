package org.prats.registries;

import org.prats.definitions.PendingDelivery;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class RetryRegistry {

    private final ConcurrentHashMap<UUID, PendingDelivery> pendingDeliveries;
    private final int maxRetries;

    public RetryRegistry(int maxRetries) {

        if (maxRetries < 0) {
            throw new IllegalArgumentException(
                    "Maximum retries cannot be negative"
            );
        }

        this.maxRetries         = maxRetries;
        this.pendingDeliveries  = new ConcurrentHashMap<>();
    }

    public boolean add(PendingDelivery pendingDelivery) {

        if (!canRetry(pendingDelivery)) {
            return false;
        }

        return pendingDeliveries.putIfAbsent(
                pendingDelivery.getCorrelationId(),
                pendingDelivery
        ) == null;
    }

    public PendingDelivery get(UUID correlationId) {
        return pendingDeliveries.get(correlationId);
    }

    public boolean remove(
            UUID correlationId,
            PendingDelivery pendingDelivery
    ) {
        return pendingDeliveries.remove(
                correlationId,
                pendingDelivery
        );
    }

    public boolean canRetry(PendingDelivery pendingDelivery) {
            return pendingDelivery.getRetryCount() < maxRetries;
    }

    public boolean registerRetryFailure(UUID correlationId) {
        PendingDelivery pendingDelivery = pendingDeliveries.get(correlationId);
        if (pendingDelivery == null) {
            return false;
        }
        synchronized (pendingDeliveries) {
            if (pendingDeliveries.get(correlationId) != pendingDelivery) {
                return false;
            }
            int retryCount = pendingDelivery.incrementRetryCount();
            if (retryCount >= maxRetries) {
                pendingDeliveries.remove(correlationId, pendingDelivery);
                return true;
            }
            return false;
        }
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public List<PendingDelivery> getAllFailures() {
        return List.copyOf(pendingDeliveries.values());
    }
}