
package org.prats.parsers;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.prats.definitions.Message;
import org.prats.definitions.MessageType;
import org.prats.definitions.Operation;
import org.prats.definitions.ProcessType;

public class CommandParser extends MessageParser {

    private final ProcessType processType;


    public CommandParser(ProcessType processType) {
        super(processType);
        this.processType = processType;
    }


    public Message createCommandMessage(String command) {

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


    public Message createUnknownCommandResponse(String invalidCommand) {

        ObjectNode payload = getObjectMapper().createObjectNode();
        payload.put("body", "UNKNOWN_COMMAND");
        payload.put("error", "Unable to identify command: " + invalidCommand);

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
