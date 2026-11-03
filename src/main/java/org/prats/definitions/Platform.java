package org.prats.definitions;

import java.util.List;

public record Platform(
        String id,
        String label,
        int tcpPort,
        int tcpBacklog,
        String mainClass,
        String classPath,
        int coreThreads,
        int maxThreads,
        int idleTimeout,
        int coreIdleTimeout,
        List<String> jvmArgs,
        List<PlatformECConfig> ecConfigs,
        String databaseConfig
) {
    public Platform {
        jvmArgs = List.copyOf(jvmArgs);
        ecConfigs = List.copyOf(ecConfigs);
    }
}