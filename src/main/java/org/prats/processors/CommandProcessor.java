package org.prats.processors;

import org.prats.definitions.classes.Message;
import org.prats.definitions.classes.ProcessContext;
import org.prats.parsers.CommandParser;
import org.prats.definitions.interfaces.ReplyChannel;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommandProcessor {

    private static final Pattern EC_LIFECYCLE_PATTERN =
            Pattern.compile(
                    "^(start|stop|restart)\\s+ec\\s+([a-zA-Z0-9][a-zA-Z0-9._-]*)$",
                    Pattern.CASE_INSENSITIVE
            );

    private final CommandParser commandParser;
    private RequestProcessor requestProcessor;
    private ResponseProcessor responseProcessor;
    private Set<String> validCommands;

    public CommandProcessor(
            CommandParser commandParser,
            ResponseProcessor responseProcessor,
            Set<String> validCommands
    ) {
        this.commandParser          = commandParser;
        this.responseProcessor      = responseProcessor;
        this.validCommands          = validCommands;
    }

    public void process(Message requestMessage, ReplyChannel replyChannel) {

        String originalCommand =
                requestMessage.getFullStringOriginalMessage();

        Message commandMessage = parseCommandToMessage(originalCommand);

        if (commandMessage == null) {
            Message errorResponse =
                    commandParser.createUnknownCommandResponse(originalCommand);
            responseProcessor.process(
                    errorResponse,
                    replyChannel
            );
            return;
        }

        requestProcessor.process(
                commandMessage,
                replyChannel
        );
    }

    private Message parseCommandToMessage(String command) {

        if (command == null || command.isBlank()) {
            return null;
        }

        String normalizedCommand = command
                .strip()
                .replaceAll("\\s+", " ");

        String lowercaseCommand = normalizedCommand
                .toLowerCase(Locale.ROOT);


        if (validCommands.contains(lowercaseCommand)) {

            return commandParser.createCommandRequest(
                    lowercaseCommand
            );
        }

        if (!ProcessContext.getProcessName().equals("platform"))
            return null;

        Matcher matcher = EC_LIFECYCLE_PATTERN.matcher(normalizedCommand);

        if (matcher.matches()) {

            String action = matcher.group(1).toLowerCase(Locale.ROOT);
            String ecId = matcher.group(2);

            String canonicalCommand = action + " ec " + ecId;

            return commandParser.createCommandRequest(
                    canonicalCommand
            );
        }

        return null;
    }

    public void setRequestProcessor(RequestProcessor requestProcessor) {
        this.requestProcessor = requestProcessor;
    }

}