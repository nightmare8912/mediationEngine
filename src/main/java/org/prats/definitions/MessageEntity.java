package org.prats.definitions;

import org.prats.inputHandler.utils.replyUtils.ReplyChannel;

public class MessageEntity {
    private Message message;
    private ReplyChannel replyChannel;
//    private String originalMessage;

    public MessageEntity(
            Message message,
            ReplyChannel replyChannel
    ) {
        this.message            = message;
        this.replyChannel       = replyChannel;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public ReplyChannel getReplyChannel() {
        return replyChannel;
    }

    public void setReplyChannel(ReplyChannel replyChannel) {
        this.replyChannel = replyChannel;
    }

//    public String getOriginalMessage() {
//        return originalMessage;
//    }
//
//    public void setOriginalMessage(String originalMessage) {
//        this.originalMessage = originalMessage;
//    }
}
