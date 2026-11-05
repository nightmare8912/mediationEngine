package org.prats.inputHandler.processors;

import org.prats.definitions.MessageEntity;
import org.prats.definitions.MessageType;
import org.prats.definitions.Operation;
import org.prats.definitions.ProcessType;
import org.prats.inputHandler.messageForwarders.*;
import org.prats.inputHandler.utils.*;
import org.prats.loggers.ShellLogger;
import org.prats.services.ServiceRegistry;
import org.prats.services.databaseService.DatabaseService;
import org.prats.services.messageService.MessageService;

public class MessageProcessor {

    private final MessageParser messageParser;
    private final ProcessType processType;
    // private MessageService messageService;

    private final DatabaseMessageForwarder databaseMessageForwarder;
    private final GUIMessageForwarder guiMessageForwarder;
    private final ECMessageForwarder ecMessageForwarder;
    private final TerminalMessageForwarder terminalMessageHandler;
    private final MiscellaneousMessageForwarder miscellaneousMessageHandler;
    private final ServiceRegistry serviceRegistry;


    public MessageProcessor(
            ProcessType processType,
            ServiceRegistry serviceRegistry
    ) {
        this.messageParser                  = new MessageParser(processType);
        this.processType                    = processType;
        this.serviceRegistry                = serviceRegistry;
        // this.messageService                 = messageService;
        this.databaseMessageForwarder       = new DatabaseMessageForwarder();


        // i think below fns need to be changed, ie, their constructor params, but let's see
        this.guiMessageForwarder            = new GUIMessageForwarder(this);
        this.ecMessageForwarder             = new ECMessageForwarder(this);
        this.terminalMessageHandler         = new TerminalMessageForwarder(this);
        this.miscellaneousMessageHandler    = new MiscellaneousMessageForwarder(this);
    }

    public ProcessType getProcessTypeDefinition() {
        return processType;
    }

    // i would generally want the message service threads to not do the complete process (ie deliver message then wait for receiver to respond and deliver it back to sender ), they should just deliver the message and return back to their pool
    // as the receiver process may take some time to respond or may not even respond, which would cause our message service threads to get stuck (leak)
    // however, in case of miscellaneous messages, the same thread can respond, as it is a miscellaneous message, we dnt know where to deliver the message so we can just send a response back
    public void forwardMessage(MessageEntity messageEntity) {

        MessageType messageType = messageEntity.getMessage().getMessageType();
        Operation operation     = messageEntity.getMessage().getOperation();

        if (messageType == MessageType.REQUEST) {
            switch (operation) {
                case FETCH, SEARCH, CREATE, UPDATE, DELETE -> databaseMessageForwarder.forwardMessage(
                        messageEntity,
                        serviceRegistry.getService(DatabaseService.class)
                );

                default -> miscellaneousMessageHandler.forwardMessage(
                        messageEntity,
                        serviceRegistry.getService(MessageService.class)
                );
            }
        }
        else if (messageType == MessageType.RESPONSE) {
            sendResponse(messageEntity);
        }
        else {
            ShellLogger.log("Invalid Message type: " + messageType);
        }
    }

    // well this reads the string message from other services and standardizes it, and then sends it over to forwardMessage from where it would be sent to dedicated handlers
    public void processMessage (MessageEntity messageEntity) {
        forwardMessage(
                messageEntity
        );
    }

    public void sendResponse(
            MessageEntity messageEntity
    ) {
        messageEntity.getReplyChannel().sendReply(
                messageParser.createJSONFromMessage(
                        messageEntity.getMessage()
                )
        );
    }
}
