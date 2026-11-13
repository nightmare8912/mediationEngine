package org.prats.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.prats.definitions.classes.Message;
import org.prats.loggers.FileLogger;
import org.prats.parsers.CommandParser;
import org.prats.definitions.interfaces.ServiceManager;
import org.prats.utilities.HealthReportFormatter;

public class ExecutionContainerCommandHandler {

    protected final ServiceManager serviceManager;
    protected final CommandParser commandParser;

    protected final String COMMAND_FIELD = "body";
    protected final String INVALID_COMMAND = "Invalid Command.";

    private final HealthReportFormatter healthReportFormatter =
            new HealthReportFormatter();

    public ExecutionContainerCommandHandler(ServiceManager serviceManager, CommandParser commandParser) {
        this.serviceManager     = serviceManager;
        this.commandParser      = commandParser;
    }

    public Message handleStatus(Message command) {
        // serviceManager.restartAll();
        trace(
                "Stepped in handleStatus",
                command
        );
        return commandParser.createCommandResponse(command, "Status OK.");
    }

    public Message handleHealthReport(Message command) {

        trace(
                "Stepped in handleHealthReport",
                command
        );

        String report = healthReportFormatter.format(
                serviceManager.getHealthReports()
        );

        return commandParser.createCommandResponse(command, report);
    }

    public Message handleRestartAll(Message command) {
        trace(
                "Stepped in handleRestartAll",
                command
        );
        serviceManager.restartAll();
        return commandParser.createCommandResponse(command, "Restarting all internal services.");
    }

    public Message handleShutdownAll(Message command) {
        trace(
                "Stepped in handleShutdownAll",
                command
        );
        serviceManager.shutdown();
        return commandParser.createCommandResponse(command, "Shutting down all services.");
    }

    public Message handleKill(Message command) {
        System.exit(0);
        return commandParser.createCommandResponse(command, "Shutting down all services.");
    }

    public Message handleInvalid(Message command) {
        return commandParser.createCommandResponse(command, INVALID_COMMAND);
    }

    public Message handleCommand(Message command) {
        String plainCommand;
        try {
            plainCommand = commandParser.extractPayloadField(command, COMMAND_FIELD, String.class);
        } catch (JsonProcessingException e) {
            FileLogger.log(
                    "[ExecutionContainerCommandHandler] [handleCommand] Error while parsing JSON" + e.getMessage()
            );
            return commandParser.createErrorResponse(
                    command.getCorrelationId(),
                    "JSON_PARSING_EXCEPTION",
                    "Error while parsing JSON"
            );
        }
        return switch (plainCommand) {
            case "status"                   -> handleStatus(command);
            case "get health report"        -> handleHealthReport(command);
            case "restart services all"     -> handleRestartAll(command);
            case "shutdown services all"    -> handleShutdownAll(command);
            case "kill"                     -> handleKill(command);
            default                         -> handleInvalid(command);
        };
    }

    protected void trace(String stage, Message message) {

        FileLogger.log(
                "[TRACE][PlatformCommandHandler][" + stage + "]"
                        + " | correlationId="
                        + (message == null ? "null" : message.getCorrelationId())
                        + " | thread="
                        + Thread.currentThread().getName()
        );
    }
}
