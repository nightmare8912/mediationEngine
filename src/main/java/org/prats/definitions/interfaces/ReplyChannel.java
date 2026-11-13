package org.prats.definitions.interfaces;

import org.prats.definitions.classes.Message;

import java.util.concurrent.CompletionStage;

public interface ReplyChannel {

    CompletionStage<Void> sendReply(Message response);

}