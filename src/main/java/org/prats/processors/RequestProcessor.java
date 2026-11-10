package org.prats.processors;

import org.prats.definitions.Message;
import org.prats.definitions.MessageType;
import org.prats.definitions.Operation;
import org.prats.parsers.MessageParser;
import org.prats.registries.CorrelationRegistry;
import org.prats.replyChannels.ReplyChannel;

public class RequestProcessor {

    private final CorrelationRegistry correlationRegistry;
    private final MessageParser messageParser;
    private final CommandProcessor commandProcessor;
    private final MessageProcessor messageProcessor;

    public RequestProcessor (
            CorrelationRegistry correlationRegistry,
            MessageParser messageParser,
            CommandProcessor commandProcessor,
            MessageProcessor messageProcessor
    ) {
        this.correlationRegistry    = correlationRegistry;
        this.messageParser          = messageParser;
        this.commandProcessor       = commandProcessor;
        this.messageProcessor       = messageProcessor;
    }


    public void process(
            Message requestMessage,
            ReplyChannel replyChannel
    ) {
        // trying to parse the incoming message to our internal message structure
        // Message requestMessage = messageParser.createMessageFromJSON(request);

        // if the parsing fails, assume it is a command and route it to command processor
        if (requestMessage.getOperation() == Operation.UNKNOWN && requestMessage.getMessageType() == MessageType.UNKNOWN) {
            commandProcessor.process(requestMessage, replyChannel);
            return;
        }

        // the parsing was successful, now this might have happened in the first try, or maybe the command processor parsed the command to internal message structure, whatever the case be we will
        // route it to message processor now as it should be able to understand it, also adding to correlation registry for response handling
        if (requestMessage.getMessageType() == MessageType.REQUEST)
            correlationRegistry.add(requestMessage.getCorrelationId(), replyChannel);

        messageProcessor.process(requestMessage);
    }
}