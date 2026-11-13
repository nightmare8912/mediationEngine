package org.prats.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.Operation;
import org.prats.loggers.FileLogger;
import org.prats.parsers.MessageParser;
import org.prats.definitions.interfaces.ReplyChannel;
import org.prats.definitions.interfaces.MessageDispatcher;

import java.util.Objects;
import java.util.function.Function;

public class TCPApplicationProcessor {

    private final MessageDispatcher messageDispatcher;
    private final MessageParser messageParser;

    public TCPApplicationProcessor(
            MessageDispatcher messageDispatcher,
            MessageParser messageParser
    ) {
        this.messageDispatcher = messageDispatcher;
        this.messageParser = messageParser;
    }

    // Outgoing with respect to the JVM
    public void processOutgoing(
            Message message,
            ClientConnection clientConnection
    ) throws Exception {
        String reply;
        if (message.getOperation() == Operation.COMMAND)
            try {
                reply = messageParser.extractPayloadField(
                        message, "body", String.class
                );
            } catch (JsonProcessingException jsonProcessingException) {
                FileLogger.log(
                        "[TCPProcessor] [processOutgoing]: " + jsonProcessingException.getMessage()
                );
                reply = messageParser.createJSONFromMessage(messageParser.createErrorResponse(
                        message.getCorrelationId(),
                        "JSON_PARSING_EXCEPTION",
                        "Internal Error"
                ));
            }
        else
            reply = messageParser.createJSONFromMessage(message);
        clientConnection.sendMessage(reply);
    }

    public void processIncoming(
            String incomingMessage,
            Function<Message, ReplyChannel> replyChannelFactory
    ) throws Exception {
        Message message = messageParser.createMessageFromJSON(
                incomingMessage
        );
        Objects.requireNonNull(incomingMessage, "Parsed message cannot be null");
        switch (message.getMessageType()) {
            case RESPONSE   ->
                    messageDispatcher.dispatchResponse(message);
            default         -> {
                ReplyChannel replyChannel =
                        replyChannelFactory.apply(message);
                Objects.requireNonNull(replyChannel);
                messageDispatcher.dispatchRequest(
                        message,
                        replyChannel
                );
            }
        }
    }

    public void processTCPRejection(
            Message rejectionMessage
    ) {
        messageDispatcher.dispatchResponse(
                rejectionMessage
        );
    }
}