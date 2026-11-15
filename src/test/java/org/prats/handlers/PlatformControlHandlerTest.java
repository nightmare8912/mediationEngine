package org.prats.handlers;

import org.junit.jupiter.api.Test;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.ProcessType;
import org.prats.parsers.CommandParser;
import org.prats.parsers.MessageParser;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PlatformControlHandlerTest {
    @org.junit.jupiter.api.BeforeAll
    static void initializeContext() {
        try { org.prats.definitions.classes.ProcessContext.getProcessName(); }
        catch (IllegalStateException e) {
            org.prats.definitions.classes.ProcessContext.initialize("platform");
        }
    }

    private final MessageParser watchdog = new MessageParser(ProcessType.WATCHDOG);
    private final PlatformControlHandler handler = new PlatformControlHandler(
            new CommandParser(ProcessType.PLATFORM), Map.of());

    @Test
    void duplicateStartDisplaysWatchdogErrorDescription() throws Exception {
        Message response = watchdog.createErrorResponse(UUID.randomUUID(),
                "UNABLE_TO_START", "The container is already running.");
        response.setStatus("FAILED");
        assertEquals("The container is already running.", handler.watchdogResponseText(response));
    }

    @Test
    void successfulStartDisplaysStatus() throws Exception {
        Message response = watchdog.createResponse(UUID.randomUUID(),
                new org.prats.definitions.records.ControlResponse("Process started. PID: 123", "Process started. PID: 123"));
        response.setStatus("SUCCESS");
        assertEquals("Process started. PID: 123", handler.watchdogResponseText(response));
    }

    @Test
    void envelopeErrorTakesPrecedenceOverSuccessStatus() throws Exception {
        Message response = watchdog.createErrorResponse(UUID.randomUUID(), "INVALID", "invalid");
        response.setStatus("SUCCESS");
        assertEquals("invalid", handler.watchdogResponseText(response));
    }
}
