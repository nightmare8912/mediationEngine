package org.prats.processors;

import org.prats.definitions.Message;
import org.prats.parsers.CommandParser;
import org.prats.replyChannels.ReplyChannel;

import java.util.Locale;
import java.util.Set;

public class CommandProcessor {

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
                .replaceAll("\\s+", " ")
                .toLowerCase(Locale.ROOT);

        if (!validCommands.contains(normalizedCommand)) {
            return null;
        }
        return commandParser.createCommandRequest(normalizedCommand);
    }

    public void setRequestProcessor(RequestProcessor requestProcessor) {
        this.requestProcessor = requestProcessor;
    }

    private void setValidCommands() {

    }
}