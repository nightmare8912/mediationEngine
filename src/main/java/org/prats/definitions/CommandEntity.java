package org.prats.definitions;

import org.prats.messageHandler.ReplyChannel;

public class CommandEntity {

    private String command;
    private ReplyChannel replyChannel;

    public CommandEntity(
            String command,
            ReplyChannel replyChannel
    ) {
        this.command = command;
        this.replyChannel       = replyChannel;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public ReplyChannel getReplyChannel() {
        return replyChannel;
    }

    public void setReplyChannel(ReplyChannel replyChannel) {
        this.replyChannel = replyChannel;
    }
}
