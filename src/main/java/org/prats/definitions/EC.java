
package org.prats.definitions;

import java.util.List;

public record EC(
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
        List<String> jvmArgs
) {
    public EC {
        jvmArgs = List.copyOf(jvmArgs);
    }
}
