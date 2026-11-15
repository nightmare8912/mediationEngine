package org.prats.parsers;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.prats.definitions.enums.ProcessType;
import org.prats.definitions.records.CommandRequest;
import org.prats.definitions.records.CommandResponse;
import org.prats.definitions.records.ControlResponse;
import org.prats.testing.TestDatabase;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class MessageContractTest {
    @BeforeAll static void context() { TestDatabase.context(); }
    private final CommandParser parser = new CommandParser(ProcessType.PLATFORM);

    @Test void commandRecordsRoundTripOnTheWire() throws Exception {
        var request = parser.createCommandRequest("mng status");
        var decoded = parser.createMessageFromJSON(parser.createJSONFromMessage(request));
        assertEquals(new CommandRequest("mng status"), parser.fromPayload(decoded.getPayload(), CommandRequest.class));
        var response = parser.createCommandResponse(decoded, "Status OK.");
        var reply = parser.createMessageFromJSON(parser.createJSONFromMessage(response));
        assertEquals(request.getCorrelationId(), reply.getCorrelationId());
        assertEquals(new CommandResponse("Status OK."), parser.fromPayload(reply.getPayload(), CommandResponse.class));
        assertFalse(reply.hasError());
    }

    @Test void errorsRoundTripInEnvelopeWithTerminalCorrelation() {
        var id = UUID.randomUUID();
        var error = parser.createErrorResponse(id, "OVERLOADED", "Retry later.");
        var decoded = parser.createMessageFromJSON(parser.createJSONFromMessage(error));
        assertEquals(id, decoded.getCorrelationId());
        assertTrue(decoded.hasError());
        assertEquals("OVERLOADED", decoded.getError().getErrorCode());
        assertEquals("Retry later.", decoded.getError().getErrorMessage());
        assertEquals("FAILED", decoded.getStatus());
        assertTrue(decoded.isResponseComplete());
        assertTrue(decoded.getPayload().isNull());
    }

    @Test void lifecyclePayloadRoundTripsAndMalformedJsonUsesEnvelopeError() throws Exception {
        var payload = new ControlResponse("Process started.", "Process started.");
        var response = parser.createResponse(UUID.randomUUID(), payload);
        var decoded = parser.createMessageFromJSON(parser.createJSONFromMessage(response));
        assertEquals(payload, parser.fromPayload(decoded.getPayload(), ControlResponse.class));
        assertTrue(parser.createMessageFromJSON("not json").hasError());
    }
}
