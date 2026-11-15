package org.prats.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.prats.definitions.classes.Message;
import org.prats.loggers.EngineLogger;
import org.prats.parsers.CommandParser;
import org.prats.definitions.interfaces.ServiceManager;
import org.prats.utilities.HealthReportFormatter;
import org.prats.definitions.classes.ProcessContext;
import org.prats.utilities.ContainerRuntime;

public class ExecutionContainerCommandHandler {

    private static final EngineLogger log =
            EngineLogger.getLogger(ExecutionContainerCommandHandler.class);

    protected final ServiceManager serviceManager;
    protected final CommandParser commandParser;

    protected final String COMMAND_FIELD = "body";
    protected final String INVALID_COMMAND = "Invalid Command.";

    private final HealthReportFormatter healthReportFormatter =
            new HealthReportFormatter();

    public ExecutionContainerCommandHandler(ServiceManager serviceManager, CommandParser commandParser) {
        this.serviceManager = serviceManager;
        this.commandParser = commandParser;

        log.debug(
                "[ExecutionContainerCommandHandler][constructor] Command handler initialized"
        );
    }

    public Message handleStatus(Message command) {
        // serviceManager.restartAll();
        trace(
                "Stepped in handleStatus",
                command
        );

        log.debug(() ->
                "[ExecutionContainerCommandHandler][handleStatus] Processing status command"
                        + " | correlationId=" + command.getCorrelationId()
        );

        return commandParser.createCommandResponse(command, "Status OK.");
    }

    protected String commandHelp() {
        return """
                MANAGEMENT COMMANDS
                  mng help                      Show this help.
                  mng status                    Show this JVM's status.
                  mng health report %s          Show this JVM's service health.
                  mng restart services all      Restart this JVM's internal services.
                  mng shutdown services all     Shut down this JVM's internal services.
                  mng kill                      Terminate this JVM (launchd may restart it).

                All commands require mng. Keywords are case insensitive; IDs retain their case.
                """.formatted(ProcessContext.getProcessName());
    }

    public Message handleHelp(Message command) {
        log.info(() -> "[ExecutionContainerCommandHandler][handleHelp] Runtime help requested"
                + " | correlationId=" + command.getCorrelationId());
        return commandParser.createCommandResponse(command, commandHelp());
    }

    public Message handleHealthReport(Message command) {

        trace(
                "Stepped in handleHealthReport",
                command
        );

        log.debug(() ->
                "[ExecutionContainerCommandHandler][handleHealthReport] Generating health report"
                        + " | correlationId=" + command.getCorrelationId()
        );

        String report = healthReportFormatter.format(
                serviceManager.getHealthReports(),
                ProcessContext.getProcessName(),
                ContainerRuntime.getStartedAt()
        );

        log.debug(() ->
                "[ExecutionContainerCommandHandler][handleHealthReport] Health report generated"
                        + " | correlationId=" + command.getCorrelationId()
                        + " | reportLength=" + report.length()
        );

        return commandParser.createCommandResponse(command, report);
    }

    public Message handleRestartAll(Message command) {
        trace(
                "Stepped in handleRestartAll",
                command
        );

        log.info(() ->
                "[ExecutionContainerCommandHandler][handleRestartAll] Restarting all internal services"
                        + " | correlationId=" + command.getCorrelationId()
        );

        serviceManager.restartAll();

        return commandParser.createCommandResponse(command, "Restarting all internal services.");
    }

    public Message handleShutdownAll(Message command) {
        trace(
                "Stepped in handleShutdownAll",
                command
        );

        log.info(() ->
                "[ExecutionContainerCommandHandler][handleShutdownAll] Shutting down all services"
                        + " | correlationId=" + command.getCorrelationId()
        );

        serviceManager.shutdown();

        return commandParser.createCommandResponse(command, "Shutting down all services.");
    }

    public Message handleKill(Message command) {

        log.warn(() ->
                "[ExecutionContainerCommandHandler][handleKill] JVM termination requested"
                        + " | correlationId=" + command.getCorrelationId()
        );

        System.exit(0);

        return commandParser.createCommandResponse(command, "Shutting down all services.");
    }

    public Message handleInvalid(Message command) {

        log.warn(() ->
                "[ExecutionContainerCommandHandler][handleInvalid] Invalid command received"
                        + " | correlationId=" + command.getCorrelationId()
        );

        return commandParser.createCommandResponse(command, INVALID_COMMAND);
    }

    public Message handleCommand(Message command) {

        log.debug(() ->
                "[ExecutionContainerCommandHandler][handleCommand] Processing command"
                        + " | correlationId=" + command.getCorrelationId()
        );

        String plainCommand;

        try {
            plainCommand = commandParser.extractPayloadField(
                    command,
                    COMMAND_FIELD,
                    String.class
            );

        } catch (JsonProcessingException e) {

            log.error(
                    "[ExecutionContainerCommandHandler][handleCommand] Error while parsing command JSON"
                            + " | correlationId=" + command.getCorrelationId(),
                    e
            );

            return commandParser.createErrorResponse(
                    command.getCorrelationId(),
                    "JSON_PARSING_EXCEPTION",
                    "Error while parsing JSON"
            );
        }

        log.debug(() ->
                "[ExecutionContainerCommandHandler][handleCommand] Command extracted"
                        + " | correlationId=" + command.getCorrelationId()
                        + " | command=" + plainCommand
        );

        if (!plainCommand.startsWith("mng ")) {
            return handleInvalid(command);
        }
        String managementCommand = plainCommand.substring(4);
        if (managementCommand.equals("health report " + ProcessContext.getProcessName())) {
            return handleHealthReport(command);
        }

        return switch (managementCommand) {
            case "help"                     -> handleHelp(command);
            case "status"                   -> handleStatus(command);
            case "restart services all"     -> handleRestartAll(command);
            case "shutdown services all"    -> handleShutdownAll(command);
            case "kill"                     -> handleKill(command);
            default                         -> handleInvalid(command);
        };
    }

    protected void trace(String stage, Message message) {

        log.debug(() ->
                "[TRACE][CommandHandler][" + stage + "]"
                        + " | correlationId="
                        + (message == null ? "null" : message.getCorrelationId())
                        + " | thread="
                        + Thread.currentThread().getName()
        );
    }
}
