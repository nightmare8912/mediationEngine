package org.prats.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.prats.definitions.classes.Message;
import org.prats.loggers.FileLogger;
import org.prats.parsers.CommandParser;
import org.prats.definitions.interfaces.ServiceManager;


public class PlatformCommandHandler extends ExecutionContainerCommandHandler {


    private final PlatformControlHandler platformControlHandler;

    public PlatformCommandHandler(
            ServiceManager serviceManager,
            CommandParser commandParser,
            PlatformControlHandler platformControlHandler
    ) {
        super(serviceManager, commandParser);
        this.platformControlHandler     = platformControlHandler;
    }



    @Override
    public Message handleCommand(Message command) {

        trace("ENTER", command);

        String plainCommand;

        // 1. Extract and normalize the command first.
        try {

            plainCommand = commandParser
                    .extractPayloadField(
                            command,
                            COMMAND_FIELD,
                            String.class
                    )
                    .trim()
                    .replaceAll("\\s+", " ");

            FileLogger.log(
                    "[TRACE][PlatformCommandHandler][COMMAND_PARSED]"
                            + " | correlationId=" + command.getCorrelationId()
                            + " | command=" + plainCommand
            );

        } catch (JsonProcessingException e) {

            FileLogger.log(
                    "[TRACE][PlatformCommandHandler][PARSE_ERROR]"
                            + " | correlationId=" + command.getCorrelationId()
                            + " | error=" + e.getMessage()
            );

            return handleInvalid(command);
        }

        // 2. Handle PLATFORM-specific EC lifecycle commands.
        if (plainCommand.startsWith("stop ec ")) {

            String ecName = plainCommand
                    .substring("stop ec ".length())
                    .trim();

            if (ecName.isBlank()) {
                return handleInvalid(command);
            }

            trace("ROUTING_TO_EC_STOP", command);

            return platformControlHandler.handleECStop(
                    command,
                    ecName
            );
        }

        if (plainCommand.startsWith("start ec ")) {

            String ecName = plainCommand
                    .substring("start ec ".length())
                    .trim();

            if (ecName.isBlank()) {
                return handleInvalid(command);
            }

            trace("ROUTING_TO_EC_START", command);

            return platformControlHandler.handleECStart(
                    command,
                    ecName
            );
        }

        if (plainCommand.startsWith("restart ec ")) {

            String ecName = plainCommand
                    .substring("restart ec ".length())
                    .trim();

            if (ecName.isBlank()) {
                return handleInvalid(command);
            }

            trace("ROUTING_TO_EC_RESTART", command);

            return platformControlHandler.handleECRestart(
                    command,
                    ecName
            );
        }

        // 3. Everything else goes to the parent exactly ONCE.
        try {

            trace("DELEGATING_TO_PARENT", command);

            Message response = super.handleCommand(command);

            FileLogger.log(
                    "[TRACE][PlatformCommandHandler][PARENT_RETURNED]"
                            + " | correlationId=" + command.getCorrelationId()
                            + " | response="
                            + (response == null ? "NULL" : "NOT NULL")
                            + " | responseCorrelationId="
                            + (response == null
                            ? "null"
                            : response.getCorrelationId())
            );

            return response;

        } catch (RuntimeException exception) {

            FileLogger.log(
                    "[TRACE][PlatformCommandHandler][PARENT_EXCEPTION]"
                            + " | correlationId=" + command.getCorrelationId()
                            + " | exception=" + exception
            );

            throw exception;
        }
    }


}
