package org.prats.definitions.interfaces;

import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public interface ControlChannelCommunicator {

    CompletionStage<Void> sendRequest(
            Message request
    );

    CompletionStage<Void> sendRequest(
            Message request,
            ReplyChannel replyChannel
    );

    void sendResponse(
            Message response
    );

    void disconnect(
            String connectionName
    );
}