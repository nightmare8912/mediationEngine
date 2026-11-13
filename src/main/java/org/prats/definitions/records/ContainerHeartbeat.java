package org.prats.definitions.records;

import org.prats.definitions.enums.HealthStatus;

import java.time.Instant;

public record ContainerHeartbeat(
        HealthStatus status,
        Instant timestamp
) {}
