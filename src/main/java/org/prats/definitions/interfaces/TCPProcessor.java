package org.prats.definitions.interfaces;


import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;
import org.prats.loggers.FileLogger;

import java.net.Socket;
import java.util.List;
import java.util.concurrent.CompletionStage;

public interface TCPProcessor {
    default List<Socket> connectedSockets() { return List.of(); }

    // processes an incoming JSON.
    void processIncoming(
            String rawMessage,
            ClientConnection connection
    ) throws Exception;

    // converts an outgoing Message into a JSON.
    String serialize(Message message);

    CompletionStage<Void> processOutgoing(
            Message outgoing,
            ClientConnection clientConnection
    );

    void informShutdown();

    void cleanupCorrelations();

    void reconcilePeers();

    boolean isPeerRegistered(String peerId);

    // called when a connection terminates.
    default void onConnectionClosed(
            ClientConnection connection
    ) {
        try {
            connection.getSocket().close();
        } catch (Exception exception) {
            FileLogger.log(
                    "Exception occured while closing socket." + exception
            );
        }
    }
}
