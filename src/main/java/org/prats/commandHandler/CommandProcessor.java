package org.prats.commandHandler;

import org.prats.definitions.CommandEntity;

public class CommandProcessor {

    public CommandProcessor() {}

    public void processStatusResponse(
            CommandEntity commandEntity
    ) {
        sendMessage(
                new CommandEntity(
                        "OK...",
                        commandEntity.getReplyChannel()
                )
        );
    }

    private void processUnknowmCommand(
            CommandEntity commandEntity
    ) {
        sendMessage(
                new CommandEntity(
                        "Unknown command: " + commandEntity.getCommand(),
                        commandEntity.getReplyChannel()
                )
        );
    }

    public void processCommand(
            CommandEntity commandEntity
    ) {
        // this is where we kind of decide what to respond...
        switch (commandEntity.getCommand()) {
            case "status":  processStatusResponse(commandEntity);
                            break;
            default:        processUnknowmCommand(commandEntity);
                            break;
        }
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
