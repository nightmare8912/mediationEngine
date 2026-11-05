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

    public InputProcessor(
            ProcessType processType,
            ServiceRegistry serviceRegistry
    ) {
        this.messageProcessor      = new MessageProcessor(
                processType,
                serviceRegistry
        );
        this.commandForwarder = new CommandForwarder();
    }

    public void processInput(
            MessageEntity messageEntity
    ) {
        if (messageEntity.getMessage().getMessageSource() == ProcessType.UNKNOWN) {
            // assuming its a command from shell, handover to command processor
            commandForwarder.processCommand(
                    new CommandEntity(
                            messageEntity.getMessage().getFullStringOriginalMessage(),
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
