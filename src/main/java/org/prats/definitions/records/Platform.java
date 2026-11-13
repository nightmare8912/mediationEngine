package org.prats.definitions.records;

import java.util.List;

public record Platform(
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
        List<String> jvmArgs,
        List<ConsolidatedWatchdogConfig> watchdogConfigs,
        List<ConsolidatedECConfig> ecConfigs,
        String databaseConfig
) {
    public Platform {
        jvmArgs = List.copyOf(jvmArgs);
        ecConfigs = List.copyOf(ecConfigs);
    }
}