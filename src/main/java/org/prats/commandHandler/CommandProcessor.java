package org.prats.commandHandler;

import org.prats.definitions.CommandEntity;

public class CommandProcessor {

    public CommandProcessor() {}

    public void processCommand(
            CommandEntity commandEntity
    ) {
        // this is where we kind of decide what to respond...
    }

    public void sendMessage(
            CommandEntity commandEntity
    ) {
        commandEntity.getReplyChannel().
                sendMessage(
                        commandEntity.getCommand()
                );
    }
}
