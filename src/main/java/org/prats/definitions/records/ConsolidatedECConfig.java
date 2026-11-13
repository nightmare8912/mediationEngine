
package org.prats.definitions.records;

public record ConsolidatedECConfig(
        String ecId,
        int tcpPort,
        int controlPort,
        int startupOrder,
        String ecConfigPath
) {}
