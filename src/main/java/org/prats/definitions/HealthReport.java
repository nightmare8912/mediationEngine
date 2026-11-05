package org.prats.definitions;

import java.time.Instant;
import java.util.Map;

public record HealthReport(
        String serviceName,
        HealthStatus status,
        String message,
        Instant timestamp,
        Map<String, Object> metrics
) {}
