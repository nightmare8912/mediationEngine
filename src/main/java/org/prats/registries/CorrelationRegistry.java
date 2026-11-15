package org.prats.registries;

import org.prats.definitions.interfaces.ReplyChannel;
import org.prats.loggers.EngineLogger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CorrelationRegistry {

    private static final EngineLogger log =
            EngineLogger.getLogger(CorrelationRegistry.class);

    private final int capacity;
    private final java.util.concurrent.Semaphore admission;
    private final java.util.concurrent.atomic.AtomicLong rejected = new java.util.concurrent.atomic.AtomicLong();
    public int capacity() { return capacity; }
    public long rejectedCount() { return rejected.get(); }

    private final ConcurrentHashMap<UUID, CorrelationEntry> correlations;

    private record CorrelationEntry(
            ReplyChannel replyChannel,
            long insertTimeNanos
    ) {}

    public record ExpiredCorrelation(
            UUID correlationId,
            ReplyChannel replyChannel
    ) {}

    private record ExpiredCandidate(
            UUID correlationId,
            CorrelationEntry entry
    ) {}

    public CorrelationRegistry() { this(Integer.getInteger("mediation.correlation.limit", 4096)); }

    public CorrelationRegistry(int capacity) {
        if (capacity < 1) throw new IllegalArgumentException("Correlation capacity must be positive");
        this.capacity = capacity;
        this.admission = new java.util.concurrent.Semaphore(capacity);
        correlations = new ConcurrentHashMap<>();

        log.debug(
                "[CorrelationRegistry][constructor] Correlation registry initialized"
        );
    }

    public void add(UUID uuid, ReplyChannel replyChannel) {
        if (replyChannel != null && replyChannel.isClosed()) throw new java.util.concurrent.RejectedExecutionException("Reply connection is closed");

        Objects.requireNonNull(uuid, "Correlation ID cannot be null");
        Objects.requireNonNull(replyChannel, "ReplyChannel cannot be null");

        log.debug(() ->
                "[CorrelationRegistry][add] Registering correlation"
                        + " | correlationId=" + uuid
                        + " | replyChannel=" + replyChannel.getClass().getSimpleName()
                        + " | currentSize=" + correlations.size()
        );

        CorrelationEntry entry = new CorrelationEntry(
                replyChannel,
                System.nanoTime()
        );

        if (!admission.tryAcquire()) {
            long count = rejected.incrementAndGet();
            if ((count & (count - 1)) == 0) log.warn("[CorrelationRegistry][add] In-flight capacity reached | limit=" + capacity + " | rejected=" + count);
            throw new java.util.concurrent.RejectedExecutionException("Correlation capacity reached: " + capacity);
        }

        if (correlations.putIfAbsent(uuid, entry) != null) {
            admission.release();

            log.warn(() ->
                    "[CorrelationRegistry][add] Duplicate correlation ID rejected"
                            + " | correlationId=" + uuid
                            + " | currentSize=" + correlations.size()
            );

            throw new IllegalStateException(
                    "Duplicate correlation ID: " + uuid
            );
        }

        log.debug(() ->
                "[CorrelationRegistry][add] Correlation registered successfully"
                        + " | correlationId=" + uuid
                        + " | currentSize=" + correlations.size()
        );
    }

    public ReplyChannel get(UUID uuid) {

        CorrelationEntry entry = correlations.get(uuid);

        log.debug(() ->
                "[CorrelationRegistry][get] Correlation lookup completed"
                        + " | correlationId=" + uuid
                        + " | correlationFound=" + (entry != null)
        );

        return entry == null ? null : entry.replyChannel();
    }

    public ReplyChannel removeAndGet(UUID uuid) {

        CorrelationEntry entry = correlations.remove(uuid);
        if (entry != null) admission.release();

        log.debug(() ->
                "[CorrelationRegistry][removeAndGet] Correlation removal completed"
                        + " | correlationId=" + uuid
                        + " | correlationFound=" + (entry != null)
                        + " | currentSize=" + correlations.size()
        );

        return entry == null ? null : entry.replyChannel();
    }

    public int getSize() {

        log.debug(() ->
                "[CorrelationRegistry][getSize] Registry size requested"
                        + " | currentSize=" + correlations.size()
        );

        return correlations.size();
    }

    public void removeForConnection(org.prats.definitions.classes.ClientConnection connection) {
        correlations.forEach((id, entry) -> {
            if (entry.replyChannel().belongsTo(connection) && correlations.remove(id, entry)) admission.release();
        });
    }

    public List<ExpiredCorrelation> removeExpired(long timeoutMillis) {

        if (timeoutMillis < 0) {
            log.warn(() ->
                    "[CorrelationRegistry][removeExpired] Invalid negative correlation timeout supplied"
                            + " | timeoutMillis=" + timeoutMillis
            );

            throw new IllegalArgumentException(
                    "Correlation timeout cannot be negative"
            );
        }

        log.debug(() ->
                "[CorrelationRegistry][removeExpired] Starting expired correlation cleanup"
                        + " | timeoutMillis=" + timeoutMillis
                        + " | registrySize=" + correlations.size()
        );

        long now = System.nanoTime();
        long timeoutNanos =
                TimeUnit.MILLISECONDS.toNanos(timeoutMillis);

        List<ExpiredCandidate> expired = new ArrayList<>();

        correlations.forEach((uuid, entry) -> {

            if (now - entry.insertTimeNanos() >= timeoutNanos) {

                if (correlations.remove(uuid, entry)) {
                    admission.release();
                    expired.add(
                            new ExpiredCandidate(uuid, entry)
                    );
                }
            }
        });

        expired.sort(
                Comparator.comparingLong(
                        candidate -> candidate.entry().insertTimeNanos()
                )
        );

        List<ExpiredCorrelation> expiredCorrelations = expired.stream()
                .map(candidate -> new ExpiredCorrelation(
                        candidate.correlationId(),
                        candidate.entry().replyChannel()
                ))
                .toList();

        log.debug(() ->
                "[CorrelationRegistry][removeExpired] Expired correlation cleanup completed"
                        + " | timeoutMillis=" + timeoutMillis
                        + " | removedCount=" + expiredCorrelations.size()
                        + " | remainingCount=" + correlations.size()
        );

        return expiredCorrelations;
    }

    // for retained sessions. where we dont want to remove the record from correlation while the session is active
    // there is ofc a better way to do it, but i dnt want to implement it now.
    public void resetTimeout(UUID uuid) {

        log.debug(() ->
                "[CorrelationRegistry][resetTimeout] Resetting correlation timeout"
                        + " | correlationId=" + uuid
        );

        correlations.computeIfPresent(uuid, (id, existing) ->
                new CorrelationEntry(existing.replyChannel(), System.nanoTime()));

        log.debug(() ->
                "[CorrelationRegistry][resetTimeout] Correlation timeout reset successfully"
                        + " | correlationId=" + uuid
        );
    }
}
