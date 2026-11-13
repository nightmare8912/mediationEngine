
package org.prats.registries;

import org.prats.definitions.classes.PendingDelivery;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RetryRegistry {

    private final ConcurrentHashMap<UUID, PendingDelivery>
            pendingDeliveries;

    private final int maxRetries;
    private final long pendingDeliveryTtlNanos;

    public RetryRegistry(
            int maxRetries,
            long pendingDeliveryTtl
    ) {

        if (maxRetries < 0) {
            throw new IllegalArgumentException(
                    "Maximum retries cannot be negative"
            );
        }

        if (pendingDeliveryTtl <= 0) {
            throw new IllegalArgumentException(
                    "Pending delivery TTL must be greater than zero"
            );
        }

        Objects.requireNonNull(
                TimeUnit.SECONDS,
                "TimeUnit cannot be null"
        );

        this.maxRetries = maxRetries;

        this.pendingDeliveryTtlNanos =
                TimeUnit.SECONDS.toNanos(pendingDeliveryTtl);

        this.pendingDeliveries = new ConcurrentHashMap<>();
    }

    public boolean add(PendingDelivery pendingDelivery) {

        Objects.requireNonNull(
                pendingDelivery,
                "Pending delivery cannot be null"
        );

        if (!canRetry(pendingDelivery)
                || isExpired(pendingDelivery)) {
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
        return pendingDelivery != null
                && pendingDelivery.getRetryCount() < maxRetries;
    }

    public boolean isExpired(PendingDelivery pendingDelivery) {
        return pendingDelivery != null
                && pendingDelivery.isExpired(
                pendingDeliveryTtlNanos
        );
    }

    public boolean registerRetryFailure(
            PendingDelivery pendingDelivery
    ) {

        Objects.requireNonNull(
                pendingDelivery,
                "Pending delivery cannot be null"
        );

        UUID correlationId = pendingDelivery.getCorrelationId();

        AtomicBoolean retryExhausted = new AtomicBoolean(false);

        pendingDeliveries.compute(correlationId, (key, current) -> {

            // Reject stale delivery callbacks.
            if (current != pendingDelivery) {
                return current;
            }

            // TTL may have expired while the delivery was in progress.
            if (isExpired(pendingDelivery)) {
                return null;
            }

            int retryCount = pendingDelivery.incrementRetryCount();

            if (retryCount >= maxRetries) {

                retryExhausted.set(true);

                return null;
            }

            return current;
        });

        return retryExhausted.get();
    }

    public List<PendingDelivery> removeExpired() {

        List<PendingDelivery> expiredDeliveries =
                new ArrayList<>();

        pendingDeliveries.forEach((correlationId, pendingDelivery) -> {

            if (isExpired(pendingDelivery)) {

                boolean removed = pendingDeliveries.remove(
                        correlationId,
                        pendingDelivery
                );

                if (removed) {
                    expiredDeliveries.add(pendingDelivery);
                }
            }
        });

        return List.copyOf(expiredDeliveries);
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public List<PendingDelivery> getAllFailures() {
        return List.copyOf(pendingDeliveries.values());
    }
}
