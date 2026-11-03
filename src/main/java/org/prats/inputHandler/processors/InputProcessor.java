package org.prats.inputHandler.processors;

import org.prats.inputHandler.messageForwarders.CommandForwarder;
import org.prats.definitions.CommandEntity;
import org.prats.definitions.MessageEntity;
import org.prats.definitions.ProcessType;
import org.prats.inputHandler.utils.MessageParser;
import org.prats.inputHandler.utils.replyUtils.ReplyChannel;
import org.prats.services.ServiceRegistry;
import org.prats.services.messageService.MessageService;

public class InputProcessor {
    private final MessageProcessor messageProcessor;
    private final CommandForwarder commandForwarder;
    private final ProcessType processType;
    private final MessageService messageService;
    private final MessageParser messageParser;

    public InputProcessor(
            ProcessType processType,
            MessageService messageService,
            ServiceRegistry serviceRegistry
    ) {
        this.processType = processType;
        this.messageService        = messageService;
        this.messageProcessor      = new MessageProcessor(
                processType,
                messageService,
                serviceRegistry
        );
        this.messageParser    = new MessageParser(processType);
        this.commandForwarder = new CommandForwarder();
    }

    public void processInput(
            MessageEntity messageEntity
    ) {
        if (messageEntity.getMessage().getMessageSource() == ProcessType.UNKNOWN) {
            // assuming its a command from shell, handover to command processor
            commandForwarder.processCommand(
                    new CommandEntity(
                            messageEntity.getOriginalMessage(),
                            messageEntity.getReplyChannel()
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
