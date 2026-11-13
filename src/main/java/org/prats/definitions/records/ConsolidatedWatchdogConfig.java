package org.prats.definitions.records;

import java.util.Objects;

public record ConsolidatedWatchdogConfig(
        String id,
        String label,
        String host,
        int controlPort,
        String configPath
) {

    public ConsolidatedWatchdogConfig {

        Objects.requireNonNull(id, "Watchdog ID cannot be null");
        Objects.requireNonNull(label, "Watchdog label cannot be null");
        Objects.requireNonNull(configPath, "Watchdog label cannot be null");

        if (id.isBlank() || label.isBlank() || configPath.isBlank()) {
            throw new IllegalArgumentException(
                    "Watchdog ID and label and config path cannot be blank"
            );
        }
    }
}