package org.prats.inputHandler.messageForwarders;

import org.prats.definitions.CommandEntity;
import org.prats.platform.Platform;

public class CommandForwarder {

    public CommandForwarder() {}

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

    private void processShutdownCommand(CommandEntity commandEntity) {
        sendMessage(
                new CommandEntity(
                        "Requested the platform for a graceful shutdown. Your connection with platform will terminate when it goes down",
                        commandEntity.getReplyChannel()
                )
        );
        Platform.shutdown();
    }

    private void processKillCommand(CommandEntity commandEntity) {
        sendMessage(
                new CommandEntity(
                        "Doing immediate shutdown of the platform and its services",
                        commandEntity.getReplyChannel()
                )
        );
        System.exit(0);
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
            case "status"           ->  processStatusResponse(commandEntity);
            case "shutdown"         ->  processShutdownCommand(commandEntity);
            case "kill"             ->  processKillCommand(commandEntity);
            default                 ->  processUnknowmCommand(commandEntity);
        }
    }

    public void sendMessage(
            CommandEntity commandEntity
    ) {
        commandEntity.getReplyChannel().
                sendReply(
                        commandEntity.getCommand()
                );
    }
}
