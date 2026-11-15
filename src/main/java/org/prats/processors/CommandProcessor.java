package org.prats.processors;

import org.prats.definitions.classes.Message;
import org.prats.definitions.classes.ProcessContext;
import org.prats.parsers.CommandParser;
import org.prats.definitions.interfaces.ReplyChannel;
import org.prats.loggers.EngineLogger;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommandProcessor {

    private static final EngineLogger log =
            EngineLogger.getLogger(CommandProcessor.class);

    private static final Pattern EC_LIFECYCLE_PATTERN =
            Pattern.compile(
                    "^(start|stop|restart)\\s+ec\\s+([a-zA-Z0-9][a-zA-Z0-9._-]*)$",
                    Pattern.CASE_INSENSITIVE
            );

    private final CommandParser commandParser;
    private static final Pattern HEALTH_REPORT_PATTERN = Pattern.compile(
            "^health report ([a-zA-Z0-9][a-zA-Z0-9._-]*)$", Pattern.CASE_INSENSITIVE);
    private RequestProcessor requestProcessor;
    private ResponseProcessor responseProcessor;
    private Set<String> validCommands;

    public CommandProcessor(
            CommandParser commandParser,
            ResponseProcessor responseProcessor,
            Set<String> validCommands
    ) {
        this.commandParser = commandParser;
        this.responseProcessor = responseProcessor;
        this.validCommands = validCommands;

        log.debug(
                "[CommandProcessor][constructor] Command processor initialized"
        );
    }

    public void process(Message requestMessage, ReplyChannel replyChannel) {

        String originalCommand =
                requestMessage.getFullStringOriginalMessage();

        log.debug(() ->
                "[CommandProcessor][process] Processing incoming command"
                        + " | command=" + originalCommand
                        + " | processName=" + ProcessContext.getProcessName()
        );

        Message commandMessage = parseCommandToMessage(originalCommand);

        if (commandMessage == null) {

            log.warn(() ->
                    "[CommandProcessor][process] Command could not be resolved; generating unknown-command response"
                            + " | command=" + originalCommand
            );

            Message errorResponse =
                    commandParser.createUnknownCommandResponse(originalCommand);

            log.debug(
                    "[CommandProcessor][process] Dispatching unknown-command response"
            );

            responseProcessor.process(
                    errorResponse,
                    replyChannel
            );

            return;
        }

        log.debug(() ->
                "[CommandProcessor][process] Command successfully parsed; forwarding to request processor"
                        + " | command=" + originalCommand
                        + " | commandMessagePresent=" + (commandMessage != null)
        );

        requestProcessor.process(
                commandMessage,
                replyChannel
        );

        log.debug(() ->
                "[CommandProcessor][process] Command forwarded to request processor"
                        + " | command=" + originalCommand
        );
    }

    private Message parseCommandToMessage(String command) {

        if (command == null || command.isBlank()) {

            log.debug(
                    "[CommandProcessor][parseCommandToMessage] Empty command received"
            );

            return null;
        }

        String normalizedCommand = command
                .strip()
                .replaceAll("\\s+", " ");

        String lowercaseCommand = normalizedCommand
                .toLowerCase(Locale.ROOT);

        if (!lowercaseCommand.startsWith("mng ")) {
            log.warn("[CommandProcessor][parseCommandToMessage] Management commands require the mng prefix");
            return null;
        }
        normalizedCommand = normalizedCommand.substring(4);
        lowercaseCommand = lowercaseCommand.substring(4);

        log.debug(() ->
                "[CommandProcessor][parseCommandToMessage] Command normalized"
                        + " | normalizedCommand=" + command
        );

        if (validCommands.contains(lowercaseCommand)) {

            log.debug(() ->
                    "[CommandProcessor][parseCommandToMessage] Command matched configured command set"
                            + " | command=" + command
            );

            return commandParser.createCommandRequest(
                    "mng " + lowercaseCommand
            );
        }

        String processName = ProcessContext.getProcessName();

        Matcher healthMatcher = HEALTH_REPORT_PATTERN.matcher(normalizedCommand);
        if (healthMatcher.matches() && (processName.equals("platform")
                || processName.equals(healthMatcher.group(1)))) {
            return commandParser.createCommandRequest("mng health report " + healthMatcher.group(1));
        }

        if (!processName.equals("platform")) {

            log.debug(() ->
                    "[CommandProcessor][parseCommandToMessage] EC lifecycle command parsing skipped outside PLATFORM"
                            + " | processName=" + processName
                            + " | command=" + command
            );

            return null;
        }

        Matcher matcher = EC_LIFECYCLE_PATTERN.matcher(normalizedCommand);

        if (matcher.matches()) {

            String action = matcher.group(1).toLowerCase(Locale.ROOT);
            String ecId = matcher.group(2);

            String canonicalCommand = "mng " + action + " ec " + ecId;

            log.info(() ->
                    "[CommandProcessor][parseCommandToMessage] PLATFORM EC lifecycle command recognized"
                            + " | action=" + action
                            + " | ecId=" + ecId
                            + " | canonicalCommand=" + canonicalCommand
            );

            return commandParser.createCommandRequest(
                    canonicalCommand
            );
        }

        log.warn(() ->
                "[CommandProcessor][parseCommandToMessage] Unrecognized command"
                        + " | processName=" + processName
                        + " | command=" + command
        );

        return null;
    }

    public void setRequestProcessor(RequestProcessor requestProcessor) {
        this.requestProcessor = requestProcessor;

        log.debug(() ->
                "[CommandProcessor][setRequestProcessor] Request processor configured"
                        + " | processorType="
                        + (requestProcessor == null
                        ? "null"
                        : requestProcessor.getClass().getSimpleName())
        );
    }
}
