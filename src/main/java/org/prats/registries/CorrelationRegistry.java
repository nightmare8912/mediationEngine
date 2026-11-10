package org.prats.registries;

import org.prats.replyChannels.ReplyChannel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CorrelationRegistry {

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

    public CorrelationRegistry() {
        correlations = new ConcurrentHashMap<>();
    }

    public void add(UUID uuid, ReplyChannel replyChannel) {

        Objects.requireNonNull(uuid, "Correlation ID cannot be null");
        Objects.requireNonNull(replyChannel, "ReplyChannel cannot be null");

        CorrelationEntry entry = new CorrelationEntry(
                replyChannel,
                System.nanoTime()
        );

        if (correlations.putIfAbsent(uuid, entry) != null) {
            throw new IllegalStateException(
                    "Duplicate correlation ID: " + uuid
            );
        }
    }

    public ReplyChannel get(UUID uuid) {

        CorrelationEntry entry = correlations.get(uuid);

        return entry == null ? null : entry.replyChannel();
    }

    public ReplyChannel removeAndGet(UUID uuid) {

        CorrelationEntry entry = correlations.remove(uuid);

        return entry == null ? null : entry.replyChannel();
    }

    public int getSize() {
        return correlations.size();
    }

    public List<ExpiredCorrelation> removeExpired(long timeoutMillis) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException(
                    "Correlation timeout cannot be negative"
            );
        }

        long now = System.nanoTime();
        long timeoutNanos =
                TimeUnit.MILLISECONDS.toNanos(timeoutMillis);

        List<ExpiredCandidate> expired = new ArrayList<>();
        correlations.forEach((uuid, entry) -> {

            if (now - entry.insertTimeNanos() >= timeoutNanos) {
                if (correlations.remove(uuid, entry)) {
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
        return expired.stream()
                .map(candidate -> new ExpiredCorrelation(
                        candidate.correlationId(),
                        candidate.entry().replyChannel()
                ))
                .toList();
    }
}