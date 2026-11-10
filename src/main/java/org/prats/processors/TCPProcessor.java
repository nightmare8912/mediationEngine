package org.prats.processors;

import org.prats.definitions.ClientConnection;
import org.prats.definitions.Message;
import org.prats.definitions.Operation;
import org.prats.parsers.MessageParser;
import org.prats.replyChannels.ReplyChannel;
import org.prats.services.MessageDispatcher;

import java.util.Objects;
import java.util.function.Function;

public class TCPProcessor {

    private final MessageDispatcher messageDispatcher;
    private final MessageParser messageParser;

    public TCPProcessor(
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
            reply = messageParser.extractPayloadField(
                    message, "body"
            );
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