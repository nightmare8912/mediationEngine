package org.prats.definitions.interfaces;

import org.prats.definitions.classes.Message;

public interface MessageDispatcher {
    void dispatchRequest(Message message, ReplyChannel replyChannel);
    void dispatchResponse(Message message);
}