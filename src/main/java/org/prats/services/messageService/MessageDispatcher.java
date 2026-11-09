package org.prats.services.messageService;

import org.prats.definitions.Message;
import org.prats.replyChannels.ReplyChannel;

public interface MessageDispatcher {
    void dispatchRequest(String message, ReplyChannel replyChannel);
    void dispatchResponse(Message message);
}
