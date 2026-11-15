package org.prats.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.prats.definitions.classes.Message;
import org.prats.loggers.EngineLogger;
import org.prats.parsers.CommandParser;
import org.prats.definitions.interfaces.ServiceManager;

public class PlatformCommandHandler extends ExecutionContainerCommandHandler {

    private static final EngineLogger log =
            EngineLogger.getLogger(PlatformCommandHandler.class);

    private final PlatformControlHandler platformControlHandler;
    private final org.prats.services.ExternalHealthMonitor externalHealthMonitor;

    public PlatformCommandHandler(
            ServiceManager serviceManager,
            CommandParser commandParser,
            PlatformControlHandler platformControlHandler,
            org.prats.services.ExternalHealthMonitor externalHealthMonitor
    ) {
        super(serviceManager, commandParser);
        this.platformControlHandler = platformControlHandler;
        this.externalHealthMonitor = externalHealthMonitor;

        log.debug(
                "[PlatformCommandHandler][constructor] Platform command handler initialized"
        );
    }


    @Override
    protected String commandHelp() {
        return super.commandHelp() + """

                PLATFORM EC COMMANDS
                  mng health report <ec-id>      Show the latest EC heartbeat and service health.
                  mng start ec <ec-id>           Start an EC through its watchdog.
                  mng stop ec <ec-id>            Stop an EC through its watchdog.
                  mng restart ec <ec-id>         Restart an EC through its watchdog.

                Example: mng health report ec01
                EC health reports show heartbeat age and flag stale or missing heartbeats.
                """;
    }

    @Override
    public Message handleCommand(Message command) {

        trace("ENTER", command);

        log.debug(() ->
                "[PlatformCommandHandler][handleCommand] Processing PLATFORM command"
                        + " | correlationId=" + command.getCorrelationId()
        );

        String plainCommand;

        // 1. Extract and normalize the command first.
        try {

            plainCommand = commandParser.fromPayload(command.getPayload(), org.prats.definitions.records.CommandRequest.class).body()
                    .trim()
                    .replaceAll("\\s+", " ");

            log.debug(() ->
                    "[PlatformCommandHandler][handleCommand] Command parsed and normalized"
                            + " | correlationId=" + command.getCorrelationId()
                            + " | command=" + plainCommand
            );

        } catch (JsonProcessingException e) {

            log.error(
                    "[PlatformCommandHandler][handleCommand] Command parsing failed"
                            + " | correlationId=" + command.getCorrelationId(),
                    e
            );

            return handleInvalid(command);
        }

        if (!plainCommand.startsWith("mng ")) {
            return handleInvalid(command);
        }
        String managementCommand = plainCommand.substring(4);
        if (managementCommand.startsWith("health report ")) {
            String ecName = managementCommand.substring("health report ".length());
            if (ecName.isBlank() || ecName.contains(" ")) {
                return handleInvalid(command);
            }
            if (ecName.equals(org.prats.definitions.classes.ProcessContext.getProcessName())) {
                return handleHealthReport(command);
            }
            log.info(() -> "[PlatformCommandHandler][handleCommand] EC health report requested"
                    + " | ecName=" + ecName + " | correlationId=" + command.getCorrelationId());
            return commandParser.createCommandResponse(command, externalHealthMonitor.healthReport(ecName));
        }

        // 2. Handle PLATFORM-specific EC lifecycle commands.
        if (managementCommand.startsWith("stop ec ")) {

            String ecName = managementCommand
                    .substring("stop ec ".length())
                    .trim();

            if (ecName.isBlank()) {

                log.warn(() ->
                        "[PlatformCommandHandler][handleCommand] EC stop command rejected because EC name is blank"
                                + " | correlationId=" + command.getCorrelationId()
                );

                return handleInvalid(command);
            }

            trace("ROUTING_TO_EC_STOP", command);

            log.info(() ->
                    "[PlatformCommandHandler][handleCommand] Routing EC stop operation"
                            + " | ecName=" + ecName
                            + " | correlationId=" + command.getCorrelationId()
            );

            return platformControlHandler.handleECStop(
                    command,
                    ecName
            );
        }

        if (managementCommand.startsWith("start ec ")) {

            String ecName = managementCommand
                    .substring("start ec ".length())
                    .trim();

            if (ecName.isBlank()) {

                log.warn(() ->
                        "[PlatformCommandHandler][handleCommand] EC start command rejected because EC name is blank"
                                + " | correlationId=" + command.getCorrelationId()
                );

                return handleInvalid(command);
            }

            trace("ROUTING_TO_EC_START", command);

            log.info(() ->
                    "[PlatformCommandHandler][handleCommand] Routing EC start operation"
                            + " | ecName=" + ecName
                            + " | correlationId=" + command.getCorrelationId()
            );

            return platformControlHandler.handleECStart(
                    command,
                    ecName
            );
        }

        if (managementCommand.startsWith("restart ec ")) {

            String ecName = managementCommand
                    .substring("restart ec ".length())
                    .trim();

            if (ecName.isBlank()) {

                log.warn(() ->
                        "[PlatformCommandHandler][handleCommand] EC restart command rejected because EC name is blank"
                                + " | correlationId=" + command.getCorrelationId()
                );

                return handleInvalid(command);
            }

            trace("ROUTING_TO_EC_RESTART", command);

            log.info(() ->
                    "[PlatformCommandHandler][handleCommand] Routing EC restart operation"
                            + " | ecName=" + ecName
                            + " | correlationId=" + command.getCorrelationId()
            );

            return platformControlHandler.handleECRestart(
                    command,
                    ecName
            );
        }

        // 3. Everything else goes to the parent exactly ONCE.
        try {

            trace("DELEGATING_TO_PARENT", command);

            log.debug(() ->
                    "[PlatformCommandHandler][handleCommand] Delegating command to parent handler"
                            + " | correlationId=" + command.getCorrelationId()
                            + " | command=" + plainCommand
            );

            Message response = super.handleCommand(command);

            log.debug(() ->
                    "[PlatformCommandHandler][handleCommand] Parent handler returned"
                            + " | correlationId=" + command.getCorrelationId()
                            + " | response=" + (response == null ? "NULL" : "NOT NULL")
                            + " | responseCorrelationId="
                            + (response == null
                            ? "null"
                            : response.getCorrelationId())
            );

            return response;

        } catch (RuntimeException exception) {

            log.error(
                    "[PlatformCommandHandler][handleCommand] Parent handler threw an exception"
                            + " | correlationId=" + command.getCorrelationId(),
                    exception
            );

            throw exception;
        }
    }
}
