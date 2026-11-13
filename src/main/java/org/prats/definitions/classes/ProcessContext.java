package org.prats.definitions.classes;

import java.util.Objects;

public final class ProcessContext {

    private static volatile String processName;

    private ProcessContext() {}

    public static void initialize(String name) {

        Objects.requireNonNull(name, "Process name cannot be null");

        if (name.isBlank()) {
            throw new IllegalArgumentException(
                    "Process name cannot be blank"
            );
        }

        synchronized (ProcessContext.class) {

            if (processName != null) {
                throw new IllegalStateException(
                        "ProcessContext is already initialized"
                );
            }

            processName = name;
        }
    }

    public static String getProcessName() {

        String name = processName;

        if (name == null) {
            throw new IllegalStateException(
                    "ProcessContext has not been initialized"
            );
        }

        return name;
    }
}
