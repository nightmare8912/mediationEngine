
package org.prats.processors;

import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.Forwarder;
import org.prats.forwarders.CommandForwarder;
import org.prats.loggers.FileLogger;

import java.util.Objects;

public class MessageProcessor {

    private final Forwarder databaseForwarder;

    private CommandForwarder commandForwarder;

    public MessageProcessor(Forwarder databaseForwarder) {
        this.databaseForwarder =
                databaseForwarder;
    }

    public void setCommandForwarder(CommandForwarder commandForwarder) {

        this.commandForwarder = Objects.requireNonNull(
                commandForwarder,
                "Command forwarder cannot be null"
        );
    }

    public void process(Message message) {

        switch (message.getOperation()) {

            case FETCH, SEARCH, CREATE, UPDATE, DELETE ->
                    databaseForwarder.forward(message);

            case COMMAND -> {

                if (commandForwarder == null) {
                    FileLogger.log(
                            "[MessageProcessor] [process] Command forwarder has not been initialized"
                    );
                    throw new IllegalStateException(
                            "Command forwarder has not been initialized"
                    );
                }

                commandForwarder.forward(message);
            }
        }
    }
}
