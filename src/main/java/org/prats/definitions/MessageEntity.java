package org.prats.definitions;

import org.prats.messageHandler.ReplyChannel;

public class MessageEntity {
    private MessageDefinition messageDefinition;
    private ReplyChannel      replyChannel;

    public MessageEntity(
            MessageDefinition messageDefinition,
            ReplyChannel replyChannel
    ) {
        this.messageDefinition  = messageDefinition;
        this.replyChannel       = replyChannel;
    }

    public MessageDefinition getMessageDefinition() {
        return messageDefinition;
    }

    public void setMessageDefinition(MessageDefinition messageDefinition) {
        this.messageDefinition = messageDefinition;
    }

    public ReplyChannel getReplyChannel() {
        return replyChannel;
    }

    public void setReplyChannel(ReplyChannel replyChannel) {
        this.replyChannel = replyChannel;
    }
}
