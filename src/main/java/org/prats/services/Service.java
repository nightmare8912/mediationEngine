package org.prats.services;

import org.prats.definitions.MessageEntity;
import org.prats.inputHandler.utils.replyUtils.ReplyChannel;

public interface Service {
    Service startService();
    void shutdownService();
    String getServiceName();
    void submit(MessageEntity messageEntity);
}
