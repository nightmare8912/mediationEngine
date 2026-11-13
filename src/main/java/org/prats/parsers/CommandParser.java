
package org.prats.parsers;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.enums.ProcessType;

public class CommandParser extends MessageParser {

    private final ProcessType processType;


    public CommandParser(ProcessType processType) {
        super(processType);
        this.processType = processType;
    }


    public Message createCommandRequest(String command) {

        ObjectNode payload = getObjectMapper().createObjectNode();
        payload.put("body", command);

        return new Message(
                MessageType.REQUEST,
                processType,
                Operation.COMMAND,
                payload,
                command,
                null
        );
    }

    public Message createCommandResponse(Message command, String response) {

        ObjectNode payload = getObjectMapper().createObjectNode();
        payload.put("body", response);

        return new Message(
                MessageType.RESPONSE,
                processType,
                Operation.COMMAND,
                payload,
                response,
                null,
                command.getCorrelationId()
        );
    }


    public Message createUnknownCommandResponse(String invalidCommand) {

        ObjectNode payload = getObjectMapper().createObjectNode();
        payload.put("errorCode", "UNKNOWN_COMMAND");
        payload.put("body", "Unable to identify command: " + invalidCommand);

        return new Message(
                MessageType.RESPONSE,
                processType,
                Operation.COMMAND,
                payload,
                invalidCommand,
                null
        );
    }
}
