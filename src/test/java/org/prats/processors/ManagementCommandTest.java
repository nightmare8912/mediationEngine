package org.prats.processors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.prats.definitions.classes.Message;
import org.prats.definitions.classes.ProcessContext;
import org.prats.definitions.enums.ProcessType;
import org.prats.parsers.CommandParser;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class ManagementCommandTest {
    @BeforeAll
    static void context() {
        try { ProcessContext.getProcessName(); }
        catch (IllegalStateException e) { ProcessContext.initialize("platform"); }
    }
    private final CommandParser parser = new CommandParser(ProcessType.PLATFORM);
    private final CommandProcessor processor = new CommandProcessor(parser, null,
            Set.of("help", "status", "restart services all", "shutdown services all", "kill"));
    private Message parse(String command) throws Exception {
        var method = CommandProcessor.class.getDeclaredMethod("parseCommandToMessage", String.class);
        method.setAccessible(true);
        return (Message) method.invoke(processor, command);
    }

    @Test
    void managementCommandsRequirePrefixAndNormalizeKeywords() throws Exception {
        for (String command : Set.of("help", "status", "restart services all", "shutdown services all", "kill",
                "start ec ec01", "stop ec ec01", "restart ec ec01",
                "health report ec01", "health report platform")) {
            assertNull(parse(command), command);
            Message parsed = parse("MNG   " + command);
            assertNotNull(parsed, command);
            assertEquals("mng " + command, parser.extractPayloadField(parsed, "body", String.class));
        }
    }

    @Test
    void oldAndMalformedHealthSyntaxIsRejected() throws Exception {
        for (String command : Set.of("mng get health report", "mng health ec ec01",
                "mng health report", "mng health report ec01 extra", "mng start ec", "mng")) {
            assertNull(parse(command), command);
        }
    }

    @Test
    void removedAccountCommandsAreRejected() throws Exception {
        for (String command : Set.of("mng register operator password", "mng login operator password",
                "mng whoami", "mng logout")) {
            assertNull(parse(command), command);
        }
    }

    @Test
    void targetIdentityRetainsCase() throws Exception {
        assertEquals("mng health report Ec-A", parser.extractPayloadField(
                parse("mNg HeAlTh RePoRt Ec-A"), "body", String.class));
    }
}
