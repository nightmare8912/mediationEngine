package org.prats.registries;

import org.prats.definitions.interfaces.ReplyChannel;
import org.prats.loggers.EngineLogger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class TCPControlRegistry {

    private static final EngineLogger log =
            EngineLogger.getLogger(TCPControlRegistry.class);

    private static final long DEFAULT_TIMEOUT_MILLIS =
            TimeUnit.MINUTES.toMillis(2);

    private final int capacity;
    private final java.util.concurrent.Semaphore admission;
    private final java.util.concurrent.atomic.AtomicLong rejected = new java.util.concurrent.atomic.AtomicLong();
    public int capacity() { return capacity; }
    public long rejectedCount() { return rejected.get(); }

    private final ConcurrentHashMap<UUID, ControlEntry> correlations;

    private record ControlEntry(
            ReplyChannel replyChannel,
            long insertTimeNanos
    ) {}

    public record ExpiredCorrelation(
            UUID correlationId,
            ReplyChannel replyChannel
    ) {}

    private record ExpiredCandidate(
            UUID correlationId,
            ControlEntry entry
    ) {}

    public TCPControlRegistry() { this(Integer.getInteger("mediation.correlation.control-limit", 1024)); }

    public TCPControlRegistry(int capacity) {
        if (capacity < 1) throw new IllegalArgumentException("Correlation capacity must be positive");
        this.capacity = capacity;
        this.admission = new java.util.concurrent.Semaphore(capacity);
        correlations = new ConcurrentHashMap<>();

        log.debug(
                "[TCPControlRegistry][constructor] TCP control registry initialized"
        );
    }

    public void add(UUID correlationId, ReplyChannel replyChannel) {

        Objects.requireNonNull(correlationId);
        Objects.requireNonNull(replyChannel);

        log.debug(() ->
                "[TCPControlRegistry][add] Registering control correlation"
                        + " | correlationId=" + correlationId
                        + " | replyChannelType=" + replyChannel.getClass().getSimpleName()
                        + " | currentSize=" + correlations.size()
        );

        ControlEntry entry = new ControlEntry(
                replyChannel,
                System.nanoTime()
        );

        if (!admission.tryAcquire()) {
            long count = rejected.incrementAndGet();
            if ((count & (count - 1)) == 0) log.warn("[TCPControlRegistry][add] In-flight capacity reached | limit=" + capacity + " | rejected=" + count);
            throw new java.util.concurrent.RejectedExecutionException("Correlation capacity reached: " + capacity);
        }

        if (correlations.putIfAbsent(correlationId, entry) != null) {
            admission.release();

            log.warn(() ->
                    "[TCPControlRegistry][add] Duplicate control correlation ID rejected"
                            + " | correlationId=" + correlationId
                            + " | currentSize=" + correlations.size()
            );

            throw new IllegalStateException(
                    "Duplicate control correlation ID: " + correlationId
            );
        }

        log.debug(() ->
                "[TCPControlRegistry][add] Control correlation registered successfully"
                        + " | correlationId=" + correlationId
                        + " | currentSize=" + correlations.size()
        );
    }

    public ReplyChannel get(UUID correlationId) {

        ControlEntry entry = correlations.get(correlationId);

        log.debug(() ->
                "[TCPControlRegistry][get] Control correlation lookup completed"
                        + " | correlationId=" + correlationId
                        + " | correlationFound=" + (entry != null)
        );

        return entry == null ? null : entry.replyChannel();
    }

    public ReplyChannel removeAndGet(UUID correlationId) {

        ControlEntry entry = correlations.remove(correlationId);
        if (entry != null) admission.release();

        log.debug(() ->
                "[TCPControlRegistry][removeAndGet] Control correlation removal completed"
                        + " | correlationId=" + correlationId
                        + " | correlationFound=" + (entry != null)
                        + " | currentSize=" + correlations.size()
        );

        return entry == null ? null : entry.replyChannel();
    }

    // Refresh timeout for retained/multiple-response sessions.
    public boolean resetTimeout(UUID correlationId) {

        boolean reset = correlations.computeIfPresent(
                correlationId,
                (id, existing) -> new ControlEntry(
                        existing.replyChannel(),
                        System.nanoTime()
                )
        ) != null;

        log.debug(() ->
                "[TCPControlRegistry][resetTimeout] Control correlation timeout refresh completed"
                        + " | correlationId=" + correlationId
                        + " | correlationFound=" + reset
        );

        return reset;
    }

    public List<ExpiredCorrelation> removeExpired() {

        log.debug(() ->
                "[TCPControlRegistry][removeExpired] Starting expiration cleanup"
                        + " | timeoutMillis=" + DEFAULT_TIMEOUT_MILLIS
                        + " | currentSize=" + correlations.size()
        );

        return removeExpired(DEFAULT_TIMEOUT_MILLIS);
    }

    public List<ExpiredCorrelation> removeExpired(long timeoutMillis) {

        if (timeoutMillis < 0) {
            log.warn(() ->
                    "[TCPControlRegistry][removeExpired] Invalid negative timeout supplied"
                            + " | timeoutMillis=" + timeoutMillis
            );

            throw new IllegalArgumentException(
                    "Timeout cannot be negative"
            );
        }

        log.debug(() ->
                "[TCPControlRegistry][removeExpired] Starting control correlation cleanup"
                        + " | timeoutMillis=" + timeoutMillis
                        + " | currentSize=" + correlations.size()
        );

        long now = System.nanoTime();

        long timeoutNanos =
                TimeUnit.MILLISECONDS.toNanos(timeoutMillis);

        List<ExpiredCandidate> expired = new ArrayList<>();

        correlations.forEach((id, entry) -> {

            if (now - entry.insertTimeNanos() >= timeoutNanos) {

                if (correlations.remove(id, entry)) {
                    admission.release();
                    expired.add(new ExpiredCandidate(id, entry));
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
                "[TCPControlRegistry][removeExpired] Control correlation cleanup completed"
                        + " | timeoutMillis=" + timeoutMillis
                        + " | removedCount=" + expiredCorrelations.size()
                        + " | remainingCount=" + correlations.size()
        );

        return expiredCorrelations;
    }

    public int getSize() {

        log.debug(() ->
                "[TCPControlRegistry][getSize] Registry size requested"
                        + " | currentSize=" + correlations.size()
        );

        return correlations.size();
    }

    public void clear() {

        int removedCount = correlations.size();

        correlations.clear();

        log.debug(() ->
                "[TCPControlRegistry][clear] TCP control registry cleared"
                        + " | removedCount=" + removedCount
        );
    }
}
