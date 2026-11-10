package org.prats.forwarders;

import org.prats.definitions.Message;
import org.prats.services.CommandDispatcher;

public class CommandForwarder implements Forwarder {

    private final CommandDispatcher commandDispatcher;

    public CommandForwarder(CommandDispatcher commandDispatcher) {
        this.commandDispatcher      = commandDispatcher;
    }

    @Override
    public void forward(Message message) {
        commandDispatcher.dispatchCommand(message);
    }
}
