
package org.prats.definitions.records;

import java.util.List;
import java.util.Objects;

public record Watchdog(
        String id,
        String label,

        String platformHost,
        int platformControlPort,

        String mainClass,
        String classPath,

        int coreThreads,
        int maxThreads,
        int idleTimeout,

        List<String> jvmArgs,

        List<ConsolidatedECConfig> ecConfigs
) {

    public Watchdog {

        Objects.requireNonNull(id, "Watchdog ID cannot be null");
        Objects.requireNonNull(label, "Watchdog label cannot be null");

        Objects.requireNonNull(
                platformHost,
                "Platform host cannot be null"
        );

        Objects.requireNonNull(
                mainClass,
                "Watchdog main class cannot be null"
        );

        Objects.requireNonNull(
                classPath,
                "Watchdog classpath cannot be null"
        );

        jvmArgs = List.copyOf(
                Objects.requireNonNull(
                        jvmArgs,
                        "JVM arguments cannot be null"
                )
        );

        ecConfigs = List.copyOf(
                Objects.requireNonNull(
                        ecConfigs,
                        "EC configurations cannot be null"
                )
        );

        if (id.isBlank() || label.isBlank()) {
            throw new IllegalArgumentException(
                    "Watchdog ID and label cannot be empty"
            );
        }

        if (platformHost.isBlank()) {
            throw new IllegalArgumentException(
                    "Platform host cannot be empty"
            );
        }

        if (platformControlPort < 1 || platformControlPort > 65535) {
            throw new IllegalArgumentException(
                    "Invalid platform control port"
            );
        }

        if (coreThreads < 0 || maxThreads < 1 || coreThreads > maxThreads) {
            throw new IllegalArgumentException(
                    "Invalid Watchdog thread configuration"
            );
        }

        if (idleTimeout < 0) {
            throw new IllegalArgumentException(
                    "Idle timeout cannot be negative"
            );
        }
    }
}
