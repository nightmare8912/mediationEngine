package org.prats.replyChannels;

import org.prats.definitions.Message;

import java.util.concurrent.CompletionStage;

public interface ReplyChannel {

    CompletionStage<Void> sendReply(Message response);

}