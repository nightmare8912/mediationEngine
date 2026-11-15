
package org.prats.definitions.records;

public record ConsolidatedECConfig(
        String ecId,
        String host,
        int tcpPort,
        int controlPort,
        int startupOrder,
        String ecConfigPath
) {}
