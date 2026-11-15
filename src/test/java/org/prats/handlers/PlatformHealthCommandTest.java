package org.prats.handlers;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.prats.definitions.classes.Message;
import org.prats.definitions.classes.ProcessContext;
import org.prats.definitions.enums.HealthStatus;
import org.prats.definitions.enums.ProcessType;
import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.definitions.records.ConsolidatedHealthReport;
import org.prats.definitions.records.HealthReport;
import org.prats.parsers.CommandParser;
import org.prats.parsers.MessageParser;
import org.prats.registries.HeartbeatRegistry;
import org.prats.services.ExternalHealthMonitor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PlatformHealthCommandTest {
    @BeforeAll
    static void initializeContext() {
        try { ProcessContext.getProcessName(); }
        catch (IllegalStateException e) { ProcessContext.initialize("platform"); }
    }

    private final MessageParser parser = new MessageParser(ProcessType.EC);
    private final HeartbeatRegistry registry = new HeartbeatRegistry(45);
    private final ExternalHealthMonitor monitor = new ExternalHealthMonitor(registry,
            List.of(new ConsolidatedECConfig("ec01", "localhost", 5100, 15001, 1, "ec1.conf")), parser);

    private Message heartbeat(String containerId, HealthStatus status) {
        Message message = parser.createEmptyMessage();
        message.setProcessName("ec01");
        Instant now = Instant.now();
        message.setPayload(parser.convertToJsonNode(new ConsolidatedHealthReport(
                containerId, status, "Service degraded", now,
                Map.of("database", new HealthReport("database", status, "Connection unavailable", now, Map.of())),
                now.minusSeconds(3600))));
        return message;
    }

    @Test
    void commandReportsHeartbeatAndServiceHealth() throws Exception {
        monitor.register(heartbeat("ec01", HealthStatus.DEGRADED));
        CommandParser commands = new CommandParser(ProcessType.PLATFORM);
        PlatformCommandHandler handler = new PlatformCommandHandler(null, commands, null, monitor);
        var processor = new org.prats.processors.CommandProcessor(commands, null, java.util.Set.of());
        var parse = processor.getClass().getDeclaredMethod("parseCommandToMessage", String.class);
        parse.setAccessible(true);
        Message request = (Message) parse.invoke(processor, "MNG   HEALTH   REPORT   ec01");
        assertNotNull(request);
        Message response = handler.handleCommand(request);
        String text = commands.extractPayloadField(response, "body", String.class);
        assertTrue(text.contains("CURRENT"));
        assertTrue(text.contains("DEGRADED"));
        assertTrue(text.contains("database"));
        assertTrue(text.contains("Connection unavailable"));
        assertFalse(text.contains("Started   : Unknown"));
        assertFalse(text.contains("Uptime    : Unknown"));
        assertEquals(HealthStatus.DEGRADED, registry.getLatestHeartbeat("ec01").orElseThrow().status());
    }

    @Test
    void runtimeHelpListsPlatformAndLocalCommands() throws Exception {
        CommandParser commands = new CommandParser(ProcessType.PLATFORM);
        var platform = new PlatformCommandHandler(null, commands, null, monitor);
        String platformHelp = commands.extractPayloadField(
                platform.handleCommand(commands.createCommandRequest("mng help")), "body", String.class);
        assertTrue(platformHelp.contains("mng health report platform"));
        assertTrue(platformHelp.contains("mng health report <ec-id>"));
        assertTrue(platformHelp.contains("mng restart ec <ec-id>"));
        String localHelp = commands.extractPayloadField(new ExecutionContainerCommandHandler(null, commands)
                .handleCommand(commands.createCommandRequest("mng help")), "body", String.class);
        assertTrue(localHelp.contains("mng help"));
        assertFalse(localHelp.contains("mng start ec"));
    }

    @Test
    void unknownAndMissingHeartbeatsAreExplicit() {
        assertTrue(monitor.healthReport("ec99").contains("Unknown EC"));
        assertTrue(monitor.healthReport("ec01").contains("no heartbeat received"));
    }

    @Test
    void mismatchedIdentityAndMissingStatusDoNotRegisterHealth() {
        monitor.register(heartbeat("ec02", HealthStatus.HEALTHY));
        monitor.register(heartbeat("ec01", null));
        assertTrue(registry.getLatestHeartbeat("ec01").isEmpty());
    }
}
