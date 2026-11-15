
package org.prats.registries;

import org.prats.definitions.interfaces.ReplyChannel;
import org.prats.loggers.EngineLogger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TCPCorrelationRegistry {

    private static final EngineLogger log =
            EngineLogger.getLogger(TCPCorrelationRegistry.class);

    private static final long DEFAULT_TIMEOUT_MILLIS =
            TimeUnit.MINUTES.toMillis(2);

    private final int capacity;
    private final Semaphore admission;
    private final AtomicLong rejected;

    private final ConcurrentHashMap<UUID, ControlEntry> correlations;

    // =========================================================
    // Internal Records
    // =========================================================

    private record ControlEntry(
            ReplyChannel replyChannel,
            long insertTimeNanos
    ) {
    }

    public record ExpiredCorrelation(
            UUID correlationId,
            ReplyChannel replyChannel
    ) {
    }

    private record ExpiredCandidate(
            UUID correlationId,
            ControlEntry entry
    ) {
    }

    // =========================================================
    // Constructors
    // =========================================================

    public TCPCorrelationRegistry() {
        this(
                Integer.getInteger(
                        "mediation.correlation.control-limit",
                        1024
                )
        );
    }

    public TCPCorrelationRegistry(int capacity) {

        if (capacity < 1) {
            throw new IllegalArgumentException(
                    "Correlation capacity must be positive"
            );
        }

        this.capacity = capacity;
        this.admission = new Semaphore(capacity);
        this.rejected = new AtomicLong();
        this.correlations = new ConcurrentHashMap<>();

        log.debug(() ->
                "[TCPControlRegistry][constructor] Registry initialized"
                        + " | capacity=" + capacity
                        + " | defaultTimeoutMillis=" + DEFAULT_TIMEOUT_MILLIS
        );
    }

    // =========================================================
    // Registration
    // =========================================================

    public void add(
            UUID correlationId,
            ReplyChannel replyChannel
    ) {

        Objects.requireNonNull(
                correlationId,
                "Correlation ID cannot be null"
        );

        Objects.requireNonNull(
                replyChannel,
                "Reply channel cannot be null"
        );

        log.debug(() ->
                "[TCPControlRegistry][add] Registering control correlation"
                        + " | correlationId=" + correlationId
                        + " | replyChannelType="
                        + replyChannel.getClass().getSimpleName()
                        + " | currentSize=" + correlations.size()
        );

        if (!admission.tryAcquire()) {

            long rejectedCount = rejected.incrementAndGet();

            if ((rejectedCount & (rejectedCount - 1)) == 0) {

                log.warn(() ->
                        "[TCPControlRegistry][add] Correlation capacity reached"
                                + " | capacity=" + capacity
                                + " | rejectedCount=" + rejectedCount
                                + " | currentSize=" + correlations.size()
                );
            }

            throw new RejectedExecutionException(
                    "Correlation capacity reached: " + capacity
            );
        }

        ControlEntry entry = new ControlEntry(
                replyChannel,
                System.nanoTime()
        );

        ControlEntry existing = correlations.putIfAbsent(
                correlationId,
                entry
        );

        if (existing != null) {

            admission.release();

            log.warn(() ->
                    "[TCPControlRegistry][add] Duplicate correlation ID rejected"
                            + " | correlationId=" + correlationId
                            + " | currentSize=" + correlations.size()
            );

            throw new IllegalStateException(
                    "Duplicate control correlation ID: " + correlationId
            );
        }

        log.debug(() ->
                "[TCPControlRegistry][add] Correlation registered successfully"
                        + " | correlationId=" + correlationId
                        + " | currentSize=" + correlations.size()
                        + " | availablePermits=" + admission.availablePermits()
        );
    }

    // =========================================================
    // Lookup
    // =========================================================

    public ReplyChannel get(UUID correlationId) {

        ControlEntry entry = correlations.get(correlationId);

        log.debug(() ->
                "[TCPControlRegistry][get] Correlation lookup completed"
                        + " | correlationId=" + correlationId
                        + " | correlationFound=" + (entry != null)
        );

        return entry == null
                ? null
                : entry.replyChannel();
    }

    // =========================================================
    // Removal
    // =========================================================

    public ReplyChannel removeAndGet(UUID correlationId) {

        ControlEntry entry = correlations.remove(correlationId);

        if (entry != null) {
            admission.release();
        }

        log.debug(() ->
                "[TCPControlRegistry][removeAndGet] Correlation removal completed"
                        + " | correlationId=" + correlationId
                        + " | correlationFound=" + (entry != null)
                        + " | currentSize=" + correlations.size()
                        + " | availablePermits=" + admission.availablePermits()
        );

        return entry == null
                ? null
                : entry.replyChannel();
    }

    // =========================================================
    // Timeout Management
    // =========================================================

    /**
     * Refreshes the timeout for an existing correlation.
     *
     * Useful for control operations that produce multiple
     * responses or progress updates.
     *
     * @return true if the correlation was found and refreshed.
     */
    public boolean resetTimeout(UUID correlationId) {

        boolean reset = correlations.computeIfPresent(
                correlationId,
                (id, existing) -> new ControlEntry(
                        existing.replyChannel(),
                        System.nanoTime()
                )
        ) != null;

        log.debug(() ->
                "[TCPControlRegistry][resetTimeout] Timeout refresh completed"
                        + " | correlationId=" + correlationId
                        + " | correlationFound=" + reset
        );

        return reset;
    }

    // =========================================================
    // Expiration Cleanup
    // =========================================================

    public List<ExpiredCorrelation> removeExpired() {
        return removeExpired(DEFAULT_TIMEOUT_MILLIS);
    }

    public List<ExpiredCorrelation> removeExpired(long timeoutMillis) {

        if (timeoutMillis < 0) {

            log.warn(() ->
                    "[TCPControlRegistry][removeExpired] Invalid timeout"
                            + " | timeoutMillis=" + timeoutMillis
            );

            throw new IllegalArgumentException(
                    "Timeout cannot be negative"
            );
        }

        log.debug(() ->
                "[TCPControlRegistry][removeExpired] Starting expiration cleanup"
                        + " | timeoutMillis=" + timeoutMillis
                        + " | currentSize=" + correlations.size()
        );

        long now = System.nanoTime();

        long timeoutNanos =
                TimeUnit.MILLISECONDS.toNanos(timeoutMillis);

        List<ExpiredCandidate> expired = new ArrayList<>();

        correlations.forEach((correlationId, entry) -> {

            long elapsedNanos =
                    now - entry.insertTimeNanos();

            if (elapsedNanos < timeoutNanos) {
                return;
            }

            boolean removed = correlations.remove(
                    correlationId,
                    entry
            );

            if (!removed) {
                return;
            }

            admission.release();

            expired.add(
                    new ExpiredCandidate(
                            correlationId,
                            entry
                    )
            );
        });

        expired.sort(
                Comparator.comparingLong(
                        candidate -> candidate.entry().insertTimeNanos()
                )
        );

        List<ExpiredCorrelation> expiredCorrelations =
                expired.stream()
                        .map(candidate -> new ExpiredCorrelation(
                                candidate.correlationId(),
                                candidate.entry().replyChannel()
                        ))
                        .toList();

        log.debug(() ->
                "[TCPControlRegistry][removeExpired] Expiration cleanup completed"
                        + " | timeoutMillis=" + timeoutMillis
                        + " | removedCount=" + expiredCorrelations.size()
                        + " | remainingCount=" + correlations.size()
                        + " | availablePermits=" + admission.availablePermits()
        );

        return expiredCorrelations;
    }

    // =========================================================
    // Registry Statistics
    // =========================================================

    public int capacity() {
        return capacity;
    }

    public long rejectedCount() {
        return rejected.get();
    }

    public int getSize() {

        int size = correlations.size();

        log.debug(() ->
                "[TCPControlRegistry][getSize] Registry size requested"
                        + " | currentSize=" + size
        );

        return size;
    }

    // =========================================================
    // Registry Cleanup
    // =========================================================

    /**
     * Removes all currently observed correlations and returns
     * their corresponding admission permits.
     *
     * Prefer calling this after stopping new registrations,
     * such as during service shutdown.
     */
    public void clear() {

        log.debug(() ->
                "[TCPControlRegistry][clear] Clearing TCP control registry"
                        + " | currentSize=" + correlations.size()
        );

        int removedCount = 0;

        for (UUID correlationId : correlations.keySet()) {

            ControlEntry removed = correlations.remove(
                    correlationId
            );

            if (removed == null) {
                continue;
            }

            admission.release();
            removedCount++;
        }

        final int clearedCount = removedCount;

        log.debug(() ->
                "[TCPControlRegistry][clear] Registry cleanup completed"
                        + " | removedCount=" + clearedCount
                        + " | remainingCount=" + correlations.size()
                        + " | availablePermits=" + admission.availablePermits()
        );
    }
}
