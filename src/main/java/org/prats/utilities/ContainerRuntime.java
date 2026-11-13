package org.prats.utilities;

import java.time.Instant;
import java.util.Objects;

public final class ContainerRuntime {

    private static volatile Instant startedAt;

    private ContainerRuntime() {}

    public static void initialize() {

        synchronized (ContainerRuntime.class) {

            if (startedAt != null) {
                throw new IllegalStateException(
                        "Container runtime is already initialized"
                );
            }

            startedAt = Instant.now();
        }
    }

    public static Instant getStartedAt() {

        return Objects.requireNonNull(
                startedAt,
                "Container runtime has not been initialized"
        );
    }
}
