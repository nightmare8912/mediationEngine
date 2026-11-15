package org.prats.registries;

import org.prats.definitions.classes.PendingDelivery;
import org.prats.loggers.EngineLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RetryRegistry {

    private static final EngineLogger log =
            EngineLogger.getLogger(RetryRegistry.class);

    private final ConcurrentHashMap<UUID, PendingDelivery>
            pendingDeliveries;

    private final int maxRetries;
    private final long pendingDeliveryTtlNanos;
    private final java.util.Map<UUID, Long> retainedBytes = new java.util.HashMap<>();
    private long pendingBytes;
    public synchronized long pendingBytes() { return pendingBytes; }
    public int size() { return pendingDeliveries.size(); }

    public RetryRegistry(
            int maxRetries,
            long pendingDeliveryTtl
    ) {

        if (maxRetries < 0) {
            log.warn(() ->
                    "[RetryRegistry][constructor] Invalid maximum retry count"
                            + " | maxRetries=" + maxRetries
            );

            throw new IllegalArgumentException(
                    "Maximum retries cannot be negative"
            );
        }

        if (pendingDeliveryTtl <= 0) {
            log.warn(() ->
                    "[RetryRegistry][constructor] Invalid pending delivery TTL"
                            + " | pendingDeliveryTtl=" + pendingDeliveryTtl
            );

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

        log.info(() ->
                "[RetryRegistry][constructor] Retry registry initialized"
                        + " | maxRetries=" + this.maxRetries
                        + " | pendingDeliveryTtlSeconds=" + pendingDeliveryTtl
                        + " | pendingDeliveryTtlNanos=" + this.pendingDeliveryTtlNanos
        );
    }

    public synchronized boolean add(PendingDelivery pendingDelivery) {
        if (pendingDelivery != null && pendingDelivery.getReplyChannel().isClosed()) return false;

        Objects.requireNonNull(
                pendingDelivery,
                "Pending delivery cannot be null"
        );

        UUID correlationId = pendingDelivery.getCorrelationId();

        log.debug(() ->
                "[RetryRegistry][add] Attempting to register pending delivery"
                        + " | correlationId=" + correlationId
                        + " | retryCount=" + pendingDelivery.getRetryCount()
                        + " | currentPendingDeliveries=" + pendingDeliveries.size()
        );

        if (!canRetry(pendingDelivery)) {

            log.debug(() ->
                    "[RetryRegistry][add] Pending delivery rejected because retry limit has been reached"
                            + " | correlationId=" + correlationId
                            + " | maxRetries=" + maxRetries
            );

            return false;
        }

        if (isExpired(pendingDelivery)) {

            log.debug(() ->
                    "[RetryRegistry][add] Pending delivery rejected because it has expired"
                            + " | correlationId=" + correlationId
            );

            return false;
        }

        long bytes = 1024L + (pendingDelivery.getResponse().getPayload() == null ? 0
                : pendingDelivery.getResponse().getPayload().toString().length() * 2L);
        if (pendingDeliveries.size() >= Integer.getInteger("mediation.retry.limit", 1024)
                || pendingBytes + bytes > Long.getLong("mediation.retry.max-bytes", 16L * 1024 * 1024)) {
            log.warn("[RetryRegistry][add] Retry capacity reached; delivery rejected");
            return false;
        }

        boolean added = pendingDeliveries.putIfAbsent(
                correlationId,
                pendingDelivery
        ) == null;
        if (added) { retainedBytes.put(correlationId, bytes); pendingBytes += bytes; }

        log.debug(() ->
                "[RetryRegistry][add] Pending delivery registration completed"
                        + " | correlationId=" + correlationId
                        + " | added=" + added
                        + " | currentPendingDeliveries=" + pendingDeliveries.size()
        );

        return added;
    }

    public PendingDelivery get(UUID correlationId) {

        PendingDelivery pendingDelivery =
                pendingDeliveries.get(correlationId);

        log.debug(() ->
                "[RetryRegistry][get] Pending delivery lookup completed"
                        + " | correlationId=" + correlationId
                        + " | deliveryFound=" + (pendingDelivery != null)
        );

        return pendingDelivery;
    }

    public synchronized boolean remove(
            UUID correlationId,
            PendingDelivery pendingDelivery
    ) {

        boolean removed = pendingDeliveries.remove(
                correlationId,
                pendingDelivery
        );
        if (removed) { Long bytes = retainedBytes.remove(correlationId); if (bytes != null) pendingBytes -= bytes; }

        log.debug(() ->
                "[RetryRegistry][remove] Pending delivery removal completed"
                        + " | correlationId=" + correlationId
                        + " | removed=" + removed
                        + " | currentPendingDeliveries=" + pendingDeliveries.size()
        );

        return removed;
    }

    public boolean canRetry(PendingDelivery pendingDelivery) {

        boolean retryAllowed = pendingDelivery != null
                && pendingDelivery.getRetryCount() < maxRetries;

        log.debug(() ->
                "[RetryRegistry][canRetry] Retry eligibility evaluated"
                        + " | correlationId="
                        + (pendingDelivery == null
                        ? null
                        : pendingDelivery.getCorrelationId())
                        + " | retryAllowed=" + retryAllowed
                        + " | maxRetries=" + maxRetries
        );

        return retryAllowed;
    }

    public synchronized void removeForConnection(org.prats.definitions.classes.ClientConnection connection) {
        pendingDeliveries.forEach((id, delivery) -> {
            if (delivery.getReplyChannel().belongsTo(connection)) remove(id, delivery);
        });
    }

    public boolean isExpired(PendingDelivery pendingDelivery) {

        boolean expired = pendingDelivery != null
                && pendingDelivery.isExpired(
                pendingDeliveryTtlNanos
        );

        log.debug(() ->
                "[RetryRegistry][isExpired] Pending delivery expiration evaluated"
                        + " | correlationId="
                        + (pendingDelivery == null
                        ? null
                        : pendingDelivery.getCorrelationId())
                        + " | expired=" + expired
        );

        return expired;
    }

    public boolean registerRetryFailure(
            PendingDelivery pendingDelivery
    ) {

        Objects.requireNonNull(
                pendingDelivery,
                "Pending delivery cannot be null"
        );

        UUID correlationId = pendingDelivery.getCorrelationId();

        log.debug(() ->
                "[RetryRegistry][registerRetryFailure] Registering retry failure"
                        + " | correlationId=" + correlationId
                        + " | retryCountBefore=" + pendingDelivery.getRetryCount()
                        + " | maxRetries=" + maxRetries
        );

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

        boolean exhausted = retryExhausted.get();

        if (exhausted) {
            log.warn(() ->
                    "[RetryRegistry][registerRetryFailure] Retry limit exhausted; pending delivery removed"
                            + " | correlationId=" + correlationId
                            + " | retryCount=" + pendingDelivery.getRetryCount()
                            + " | maxRetries=" + maxRetries
                            + " | currentPendingDeliveries=" + pendingDeliveries.size()
            );
        } else {
            log.debug(() ->
                    "[RetryRegistry][registerRetryFailure] Retry failure registration completed"
                            + " | correlationId=" + correlationId
                            + " | retryCount=" + pendingDelivery.getRetryCount()
                            + " | retryExhausted=false"
                            + " | currentPendingDeliveries=" + pendingDeliveries.size()
            );
        }

        return exhausted;
    }

    public List<PendingDelivery> removeExpired() {

        log.debug(() ->
                "[RetryRegistry][removeExpired] Starting pending delivery expiration cleanup"
                        + " | currentPendingDeliveries=" + pendingDeliveries.size()
        );

        List<PendingDelivery> expiredDeliveries =
                new ArrayList<>();

        pendingDeliveries.forEach((correlationId, pendingDelivery) -> {

            if (isExpired(pendingDelivery)) {

                boolean removed = remove(
                        correlationId,
                        pendingDelivery
                );

                if (removed) {
                    expiredDeliveries.add(pendingDelivery);
                }
            }
        });

        List<PendingDelivery> result = List.copyOf(expiredDeliveries);

        log.debug(() ->
                "[RetryRegistry][removeExpired] Pending delivery expiration cleanup completed"
                        + " | removedCount=" + result.size()
                        + " | remainingCount=" + pendingDeliveries.size()
        );

        return result;
    }

    public int getMaxRetries() {

        log.debug(() ->
                "[RetryRegistry][getMaxRetries] Maximum retry count requested"
                        + " | maxRetries=" + maxRetries
        );

        return maxRetries;
    }

    public List<PendingDelivery> getAllFailures() {

        List<PendingDelivery> snapshot =
                List.copyOf(pendingDeliveries.values());

        log.debug(() ->
                "[RetryRegistry][getAllFailures] Pending delivery snapshot retrieved"
                        + " | failureCount=" + snapshot.size()
        );

        return snapshot;
    }
}
