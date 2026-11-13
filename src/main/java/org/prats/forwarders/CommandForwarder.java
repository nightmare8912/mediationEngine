package org.prats.forwarders;

import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.CommandDispatcher;
import org.prats.definitions.interfaces.Forwarder;

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
