package org.prats.inputHandler;

import org.prats.commandHandler.CommandProcessor;
import org.prats.definitions.CommandEntity;
import org.prats.definitions.MessageEntity;
import org.prats.definitions.ProcessType;
import org.prats.messageHandler.MessageParser;
import org.prats.messageHandler.MessageProcessor;
import org.prats.messageHandler.ReplyChannel;
import org.prats.services.messageService.MessageService;

public class InputProcessor {
    private final MessageProcessor messageProcessor;
    private final CommandProcessor commandProcessor;
    private final ProcessType processType;
    private final MessageService messageService;
    private final MessageParser messageParser;

    public InputProcessor(
            ProcessType processType,
            MessageService messageService
    ) {
        this.processType = processType;
        this.messageService        = messageService;
        this.messageProcessor      = new MessageProcessor(
                processType,
                messageService
        );
        this.messageParser    = new MessageParser(processType);
        this.commandProcessor = new CommandProcessor();
    }

    public void processInput(
            String input,
            ReplyChannel replyChannel
    ) {
        MessageEntity messageEntity = new MessageEntity(
                messageParser.createMessageFromJSON(input),
                replyChannel
        );
        if (messageEntity.getMessageDefinition().getMessageSource() == ProcessType.UNKNOWN) {
            // assuming its a command from shell, handover to command processor
            commandProcessor.processCommand(
                    new CommandEntity(
                        input,
                        replyChannel
                    )
            );
        }
        else
        {
            messageProcessor.processMessage(
                    messageEntity
            );
        }
    }
}
