package org.prats.definitions.records;

public record TCPPeer(
        String processId,
        String host,
        int port
) {}