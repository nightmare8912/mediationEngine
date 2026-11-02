package org.prats.messageHandler;

import org.prats.definitions.MessageEntity;
import org.prats.definitions.ProcessType;
import org.prats.services.messageService.MessageService;

public class MessageProcessor {

    private final MessageParser messageParser;
    private final ProcessType processType;
    // private MessageService messageService;

    private final DatabaseMessageHandler databaseMessageHandler;
    private final GUIMessageHandler guiMessageHandler;
    private final ECMessageHandler ecMessageHandler;
    private final TerminalMessageHandler terminalMessageHandler;
    private final MiscellaneousMessageHandler miscellaneousMessageHandler;


    public MessageProcessor(
            ProcessType processType,
            MessageService messageService
    ) {
        this.messageParser                  = new MessageParser(processType);
        this.processType = processType;
        // this.messageService                 = messageService;
        this.databaseMessageHandler         = new DatabaseMessageHandler(this);
        this.guiMessageHandler              = new GUIMessageHandler(this);
        this.ecMessageHandler               = new ECMessageHandler(this);
        this.terminalMessageHandler         = new TerminalMessageHandler(this);
        this.miscellaneousMessageHandler    = new MiscellaneousMessageHandler(this);
    }

    public ProcessType getProcessTypeDefinition() {
        return processType;
    }

    // i would generally want the message service threads to not do the complete process (ie deliver message then wait for receiver to respond and deliver it back to sender ), they should just deliver the message and return back to their pool
    // as the receiver process may take some time to respond or may not even respond, which would cause our message service threads to get stuck (leak)
    // however, in case of miscellaneous messages, the same thread can respond, as it is a miscellaneous message, we dnt know where to deliver the message so we can just send a response back
    public void forwardMessage(MessageEntity messageEntity) {

        ProcessType sourceProcess = messageEntity.getMessageDefinition().getMessageSource();

        if (sourceProcess == ProcessType.DATABASE) {
            // forward to database/sql message handler
            databaseMessageHandler.handleMessage(
                    messageEntity
            );
        }
        else if (sourceProcess == ProcessType.GUI) {
            // forward to gui message handler
            guiMessageHandler.handleMessage(
                    messageEntity
            );
        }
        else if (sourceProcess == ProcessType.EC) {
            // forward to ec message handler
            ecMessageHandler.handleMessage(
                    messageEntity
            );
        }
        else if (sourceProcess == ProcessType.TERMINAL) {
            // forward to terminal message handler
            terminalMessageHandler.handleMessage(
                    messageEntity
            );
        }
        else  {
            // forward to miscellaneous message handler
            miscellaneousMessageHandler.handleMessage(
                    messageEntity
            );
        }

    }

    // well this reads the string message from other services and standardizes it, and then sends it over to forwardMessage from where it would be sent to dedicated handlers
    public void processMessage (MessageEntity messageEntity) {
        forwardMessage(
                messageEntity
        );
    }

    public void sendMessage(
            MessageEntity messageEntity
    ) {
        messageEntity.getReplyChannel().sendMessage(
                messageParser.createJSONFromMessage(
                        messageEntity.getMessageDefinition()
                )
        );
    }
}
