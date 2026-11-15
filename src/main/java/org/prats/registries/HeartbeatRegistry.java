package org.prats.registries;

import org.prats.definitions.records.ContainerHeartbeat;
import org.prats.definitions.enums.HealthStatus;
import org.prats.loggers.EngineLogger;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class HeartbeatRegistry {

    private static final EngineLogger log =
            EngineLogger.getLogger(HeartbeatRegistry.class);

    private static final int MAX_HEARTBEATS_PER_CONTAINER = 50;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private final long ttlNanos;

    private final ConcurrentMap<String, ContainerHistory>
            heartbeatHistoryByContainer = new ConcurrentHashMap<>();

    public HeartbeatRegistry(long ttlSeconds) {

        if (ttlSeconds <= 0) {
            log.warn(() ->
                    "[HeartbeatRegistry][constructor] Invalid heartbeat TTL"
                            + " | ttlSeconds=" + ttlSeconds
            );

            throw new IllegalArgumentException(
                    "TTL must be greater than zero seconds"
            );
        }

        try {
            this.ttlNanos = Math.multiplyExact(
                    ttlSeconds,
                    NANOS_PER_SECOND
            );
        } catch (ArithmeticException e) {
            log.error(
                    "[HeartbeatRegistry][constructor] Heartbeat TTL is too large"
                            + " | ttlSeconds=" + ttlSeconds,
                    e
            );

            throw new IllegalArgumentException(
                    "TTL is too large",
                    e
            );
        }

        log.info(() ->
                "[HeartbeatRegistry][constructor] Heartbeat registry initialized"
                        + " | ttlSeconds=" + ttlSeconds
                        + " | maxHeartbeatsPerContainer=" + MAX_HEARTBEATS_PER_CONTAINER
        );
    }

    public void register(
            String containerName,
            HealthStatus healthStatus
    ) {

        validateContainerName(containerName);
        Objects.requireNonNull(
                healthStatus,
                "Health status cannot be null"
        );

        log.debug(() ->
                "[HeartbeatRegistry][register] Registering container heartbeat"
                        + " | containerName=" + containerName
                        + " | healthStatus=" + healthStatus
                        + " | registeredContainers=" + heartbeatHistoryByContainer.size()
        );

        heartbeatHistoryByContainer.compute(
                containerName,
                (name, history) -> {

                    if (history == null) {
                        history = new ContainerHistory();
                    }

                    history.add(healthStatus);

                    return history;
                }
        );

        log.debug(() ->
                "[HeartbeatRegistry][register] Container heartbeat registered"
                        + " | containerName=" + containerName
                        + " | healthStatus=" + healthStatus
                        + " | registeredContainers=" + heartbeatHistoryByContainer.size()
        );
    }

    public Optional<ContainerHeartbeat> getLatestHeartbeat(
            String containerName
    ) {

        validateContainerName(containerName);

        ContainerHistory history =
                heartbeatHistoryByContainer.get(containerName);

        if (history == null) {
            log.debug(() ->
                    "[HeartbeatRegistry][getLatestHeartbeat] No heartbeat history found"
                            + " | containerName=" + containerName
            );

            return Optional.empty();
        }

        Optional<ContainerHeartbeat> latest = history.getLatest();

        log.debug(() ->
                "[HeartbeatRegistry][getLatestHeartbeat] Latest heartbeat lookup completed"
                        + " | containerName=" + containerName
                        + " | heartbeatFound=" + latest.isPresent()
        );

        return latest;
    }

    public List<ContainerHeartbeat> getHistory(
            String containerName
    ) {

        validateContainerName(containerName);

        ContainerHistory history =
                heartbeatHistoryByContainer.get(containerName);

        if (history == null) {
            log.debug(() ->
                    "[HeartbeatRegistry][getHistory] No heartbeat history found"
                            + " | containerName=" + containerName
            );

            return List.of();
        }

        List<ContainerHeartbeat> snapshot = history.getSnapshot();

        log.debug(() ->
                "[HeartbeatRegistry][getHistory] Heartbeat history snapshot retrieved"
                        + " | containerName=" + containerName
                        + " | heartbeatCount=" + snapshot.size()
        );

        return snapshot;
    }

    public boolean isExpired(String containerName) {

        validateContainerName(containerName);

        ContainerHistory history =
                heartbeatHistoryByContainer.get(containerName);

        if (history == null) {
            log.debug(() ->
                    "[HeartbeatRegistry][isExpired] Container has no registered heartbeat history"
                            + " | containerName=" + containerName
                            + " | expired=true"
            );

            return true;
        }

        boolean expired = history.isExpired(
                System.nanoTime(),
                ttlNanos
        );

        log.debug(() ->
                "[HeartbeatRegistry][isExpired] Heartbeat expiration check completed"
                        + " | containerName=" + containerName
                        + " | expired=" + expired
        );

        return expired;
    }

    public int cleanupExpiredHistory() {

        log.debug(() ->
                "[HeartbeatRegistry][cleanupExpiredHistory] Starting heartbeat history cleanup"
                        + " | registeredContainers=" + heartbeatHistoryByContainer.size()
        );

        long currentTimeNanos = System.nanoTime();

        int removedRecords = 0;

        for (ContainerHistory history :
                heartbeatHistoryByContainer.values()) {

            removedRecords += history.removeExpiredHistory(
                    currentTimeNanos,
                    ttlNanos
            );
        }

        final int cleanedRecords = removedRecords;

        log.debug(() ->
                "[HeartbeatRegistry][cleanupExpiredHistory] Heartbeat history cleanup completed"
                        + " | removedRecords=" + cleanedRecords
                        + " | registeredContainers=" + heartbeatHistoryByContainer.size()
        );

        return removedRecords;
    }

    public void removeContainer(String containerName) {

        validateContainerName(containerName);

        ContainerHistory removedHistory =
                heartbeatHistoryByContainer.remove(containerName);

        log.debug(() ->
                "[HeartbeatRegistry][removeContainer] Container heartbeat history removal completed"
                        + " | containerName=" + containerName
                        + " | historyFound=" + (removedHistory != null)
                        + " | registeredContainers=" + heartbeatHistoryByContainer.size()
        );
    }

    private static void validateContainerName(String containerName) {

        Objects.requireNonNull(
                containerName,
                "Container name cannot be null"
        );

        if (containerName.isBlank()) {
            throw new IllegalArgumentException(
                    "Container name cannot be blank"
            );
        }
    }

    private static final class ContainerHistory {

        private final Deque<StoredHeartbeat> heartbeats =
                new ArrayDeque<>(MAX_HEARTBEATS_PER_CONTAINER);

        private synchronized void add(HealthStatus status) {

            ContainerHeartbeat heartbeat =
                    new ContainerHeartbeat(
                            status,
                            Instant.now()
                    );

            StoredHeartbeat storedHeartbeat =
                    new StoredHeartbeat(
                            heartbeat,
                            System.nanoTime()
                    );

            if (heartbeats.size() == MAX_HEARTBEATS_PER_CONTAINER) {
                heartbeats.removeFirst();
            }

            heartbeats.addLast(storedHeartbeat);
        }

        private synchronized Optional<ContainerHeartbeat> getLatest() {

            StoredHeartbeat latest = heartbeats.peekLast();

            if (latest == null) {
                return Optional.empty();
            }

            return Optional.of(latest.heartbeat());
        }

        private synchronized List<ContainerHeartbeat> getSnapshot() {

            List<ContainerHeartbeat> snapshot =
                    new ArrayList<>(heartbeats.size());

            for (StoredHeartbeat heartbeat : heartbeats) {
                snapshot.add(heartbeat.heartbeat());
            }

            return List.copyOf(snapshot);
        }

        private synchronized boolean isExpired(
                long currentTimeNanos,
                long ttlNanos
        ) {

            StoredHeartbeat latest = heartbeats.peekLast();

            if (latest == null) {
                return true;
            }

            return currentTimeNanos - latest.receivedAtNanos()
                    >= ttlNanos;
        }

        private synchronized int removeExpiredHistory(
                long currentTimeNanos,
                long ttlNanos
        ) {

            int removed = 0;

            while (heartbeats.size() > 1) {

                StoredHeartbeat oldest = heartbeats.peekFirst();

                if (currentTimeNanos - oldest.receivedAtNanos()
                        < ttlNanos) {
                    break;
                }

                heartbeats.removeFirst();
                removed++;
            }

            return removed;
        }
    }

    private record StoredHeartbeat(
            ContainerHeartbeat heartbeat,
            long receivedAtNanos
    ) {}
}