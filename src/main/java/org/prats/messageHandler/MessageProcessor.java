package org.prats.messageHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.prats.definitions.MessageDefinition;
import org.prats.loggers.ShellLogger;
import org.prats.platform.Platform;
import org.prats.platform.tcpService.ClientConnection;

import java.net.Socket;

public class MessageProcessor {

    private final MessageParser messageParser;
    private final PingResponder pingResponder;
    private final MessageSource messageSource;

    public MessageProcessor(MessageSource messageSource) {
        this.messageParser = new MessageParser();
        this.pingResponder = new PingResponder();
        this.messageSource = messageSource;
    }

    public MessageDefinition createMessage(
            MessageType     messageType,
            MessageSource   messageSource,
            String          message
    ) {
        return new MessageDefinition(messageType, messageSource, message);
    }

    public void forwardMessage(MessageDefinition message, ClientConnection clientConnection)
            throws JsonProcessingException {

        if (message.getMessageSource() == MessageSource.DATABASE) {
            // forward to database/sql message handler
        }
        else if (message.getMessageSource() == MessageSource.GUI) {
            // forward to gui message handler
        }
        else if (message.getMessageSource() == MessageSource.EC) {
            // forward to ec message handler
        }
        else if (message.getMessageSource() == MessageSource.TERMINAL) {

        }
        else  {
            sendMessage(
                new MessageDefinition(
                        MessageType.ACK,
                        messageSource,
                        "Running"
                ),
                clientConnection
            );
        }

    }

    public void sendMessage(
            MessageDefinition messageDefinition,
            ClientConnection clientConnection
    ) {
        try {
            messageParser.createJSONFromMessage(messageDefinition);
            clientConnection.sendMessage(messageParser.createJSONFromMessage(messageDefinition));
        } catch (Exception exe) {
            ShellLogger.log(exe.getMessage());
        }
    }

    public void processMessage (String message, ClientConnection clientConnection)
            throws JsonProcessingException {
        forwardMessage(messageParser.createMessageFromJSON(message), clientConnection);
    }

}
