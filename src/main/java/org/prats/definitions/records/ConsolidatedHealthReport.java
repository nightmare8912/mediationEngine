package org.prats.definitions.records;

import org.prats.definitions.enums.HealthStatus;

import java.time.Instant;
import java.util.Map;

public record ConsolidatedHealthReport(
        String containerId,
        HealthStatus overallStatus,
        String overallMessage,
        Instant timestamp,
        Map<String, HealthReport> services,
        Instant startedAt
) {
    public ConsolidatedHealthReport(String containerId, HealthStatus overallStatus, String overallMessage,
                                    Instant timestamp, Map<String, HealthReport> services) {
        this(containerId, overallStatus, overallMessage, timestamp, services, null);
    }
}
