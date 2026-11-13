package org.prats.registries;

import org.prats.definitions.interfaces.ReplyChannel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class TCPControlRegistry {

    private static final long DEFAULT_TIMEOUT_MILLIS =
            TimeUnit.MINUTES.toMillis(2);

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

    public TCPControlRegistry() {
        correlations = new ConcurrentHashMap<>();
    }

    public void add(UUID correlationId, ReplyChannel replyChannel) {

        Objects.requireNonNull(correlationId);
        Objects.requireNonNull(replyChannel);

        ControlEntry entry = new ControlEntry(
                replyChannel,
                System.nanoTime()
        );

        if (correlations.putIfAbsent(correlationId, entry) != null) {
            throw new IllegalStateException(
                    "Duplicate control correlation ID: " + correlationId
            );
        }
    }

    public ReplyChannel get(UUID correlationId) {

        ControlEntry entry = correlations.get(correlationId);

        return entry == null ? null : entry.replyChannel();
    }

    public ReplyChannel removeAndGet(UUID correlationId) {

        ControlEntry entry = correlations.remove(correlationId);

        return entry == null ? null : entry.replyChannel();
    }

    // Refresh timeout for retained/multiple-response sessions.
    public boolean resetTimeout(UUID correlationId) {

        return correlations.computeIfPresent(
                correlationId,
                (id, existing) -> new ControlEntry(
                        existing.replyChannel(),
                        System.nanoTime()
                )
        ) != null;
    }

    public List<ExpiredCorrelation> removeExpired() {
        return removeExpired(DEFAULT_TIMEOUT_MILLIS);
    }

    public List<ExpiredCorrelation> removeExpired(long timeoutMillis) {

        if (timeoutMillis < 0) {
            throw new IllegalArgumentException(
                    "Timeout cannot be negative"
            );
        }

        long now = System.nanoTime();

        long timeoutNanos =
                TimeUnit.MILLISECONDS.toNanos(timeoutMillis);

        List<ExpiredCandidate> expired = new ArrayList<>();

        correlations.forEach((id, entry) -> {

            if (now - entry.insertTimeNanos() >= timeoutNanos) {

                if (correlations.remove(id, entry)) {
                    expired.add(new ExpiredCandidate(id, entry));
                }
            }
        });

        expired.sort(
                Comparator.comparingLong(
                        candidate -> candidate.entry().insertTimeNanos()
                )
        );

        return expired.stream()
                .map(candidate -> new ExpiredCorrelation(
                        candidate.correlationId(),
                        candidate.entry().replyChannel()
                ))
                .toList();
    }

    public int getSize() {
        return correlations.size();
    }

    public void clear() {
        correlations.clear();
    }
}