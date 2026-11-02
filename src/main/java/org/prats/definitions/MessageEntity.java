package org.prats.definitions;

import org.prats.messageHandler.ReplyChannel;

public class MessageEntity {
    private Message message;
    private ReplyChannel      replyChannel;

    public MessageEntity(
            Message message,
            ReplyChannel replyChannel
    ) {
        this.message = message;
        this.replyChannel       = replyChannel;
    }

    public Message getMessageDefinition() {
        return message;
    }

    public void setMessageDefinition(Message message) {
        this.message = message;
    }

    public ReplyChannel getReplyChannel() {
        return replyChannel;
    }

    public void setReplyChannel(ReplyChannel replyChannel) {
        this.replyChannel = replyChannel;
    }
}
