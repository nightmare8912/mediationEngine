
package org.prats.definitions.records;

import java.util.List;

public record EC(
        String id,
        String label,
        int tcpPort,
        int tcpBacklog,
        int controlPort,
        String mainClass,
        String classPath,
        int coreThreads,
        int maxThreads,
        int idleTimeout,
        int coreIdleTimeout,
        boolean databaseConfigured,
        String dbConfigPath,
        List<String> jvmArgs,
        List<ConsolidatedECConfig> ecConfigs
) {

    public EC {
        jvmArgs = List.copyOf(jvmArgs);
        ecConfigs = List.copyOf(ecConfigs);

        if (databaseConfigured &&
                (dbConfigPath == null || dbConfigPath.isBlank())) {

            throw new IllegalArgumentException(
                    "Database configuration is enabled, "
                            + "but dbConfigPath is missing for EC: " + id
            );
        }

        if (!databaseConfigured &&
                dbConfigPath != null &&
                !dbConfigPath.isBlank()) {

            throw new IllegalArgumentException(
                    "dbConfigPath was supplied even though database "
                            + "configuration is disabled for EC: " + id
            );
        }
    }
}
