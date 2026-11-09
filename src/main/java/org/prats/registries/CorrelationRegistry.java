package org.prats.registries;

import org.prats.replyChannels.ReplyChannel;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CorrelationRegistry {
    private final ConcurrentHashMap<UUID, CorrelationEntry> correlations;

    private record CorrelationEntry(
            ReplyChannel replyChannel,
            Instant insertTime
    ) {}

    public CorrelationRegistry() {
        correlations    = new ConcurrentHashMap<>();
    }

    public void add(UUID uuid, ReplyChannel replyChannel) {
        correlations.put(uuid, new CorrelationEntry(replyChannel, Instant.now()));
    }

    public ReplyChannel removeAndGet(UUID uuid) {
        return correlations.remove(uuid).replyChannel();
    }

    public ReplyChannel get(UUID uuid) {
        return correlations.get(uuid).replyChannel();
    }

    public int getSize() { return correlations.size(); }

    public List<UUID> getExpiredUUIDs(long correlationTimeout) {

        Instant now = Instant.now();

        return correlations.entrySet().stream()
                .filter(entry ->
                        Duration.between(
                                entry.getValue().insertTime(),
                                now
                        ).toMillis() >= correlationTimeout
                )
                .sorted(Map.Entry.comparingByValue(
                        Comparator.comparing(CorrelationEntry::insertTime)
                ))
                .map(Map.Entry::getKey)
                .toList();
    }
}
