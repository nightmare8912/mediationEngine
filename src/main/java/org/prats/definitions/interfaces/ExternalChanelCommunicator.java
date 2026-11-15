package org.prats.definitions.interfaces;

import org.prats.definitions.classes.Message;

import java.util.concurrent.CompletionStage;

public interface ExternalChanelCommunicator {

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