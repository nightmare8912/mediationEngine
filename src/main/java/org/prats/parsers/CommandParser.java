
package org.prats.parsers;

import org.prats.definitions.records.CommandRequest;
import org.prats.definitions.records.CommandResponse;

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

        var payload = toPayload(new CommandRequest(command));

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

        var payload = toPayload(new CommandResponse(response));

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

        Message response = createErrorResponse(java.util.UUID.randomUUID(),
                "UNKNOWN_COMMAND", "Unable to identify command: " + invalidCommand);
        response.setOperation(Operation.COMMAND);
        return response;
    }
}
