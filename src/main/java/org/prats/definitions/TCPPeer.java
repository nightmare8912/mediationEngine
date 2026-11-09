package org.prats.definitions;

public record TCPPeer(
        String processId,
        String host,
        int port
) {}