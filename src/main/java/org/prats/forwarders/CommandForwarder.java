package org.prats.forwarders;

import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.CommandDispatcher;
import org.prats.definitions.interfaces.Forwarder;
import org.prats.loggers.EngineLogger;

public class CommandForwarder implements Forwarder {

    private static final EngineLogger log =
            EngineLogger.getLogger(CommandForwarder.class);

    private final CommandDispatcher commandDispatcher;

    public CommandForwarder(CommandDispatcher commandDispatcher) {
        this.commandDispatcher = commandDispatcher;

        log.debug(
                "[CommandForwarder][constructor] Command forwarder initialized"
        );
    }

    @Override
    public void forward(Message message) {

        log.debug(() ->
                "[CommandForwarder][forward] Forwarding command"
                        + " | operation=" + message.getOperation()
                        + " | correlationId=" + message.getCorrelationId()
        );

        commandDispatcher.dispatchCommand(message);

        log.debug(() ->
                "[CommandForwarder][forward] Command dispatched successfully"
                        + " | operation=" + message.getOperation()
                        + " | correlationId=" + message.getCorrelationId()
        );
    }
}