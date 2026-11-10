package org.prats.services;

import org.prats.definitions.Message;
import org.prats.replyChannels.ReplyChannel;

public interface MessageDispatcher {
    void dispatchRequest(Message message, ReplyChannel replyChannel);
    void dispatchResponse(Message message);
}