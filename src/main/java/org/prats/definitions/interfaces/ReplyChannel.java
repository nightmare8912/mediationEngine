package org.prats.definitions.interfaces;

import org.prats.definitions.classes.Message;

import java.util.concurrent.CompletionStage;

public interface ReplyChannel {
    default boolean isClosed() { return false; }
    default boolean belongsTo(org.prats.definitions.classes.ClientConnection connection) { return false; }

    CompletionStage<Void> sendReply(Message response);

}
