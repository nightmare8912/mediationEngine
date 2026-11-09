package org.prats.replyChannels;

import java.util.concurrent.CompletionStage;

public interface ReplyChannel {
    CompletionStage<Void> sendReply(String reply) throws Exception;
}
