package org.prats.utilities;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.nio.file.Files;
import java.time.Instant;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.*;

class ECProcessIdentityStoreTest {
    @TempDir Path home;

    @Test
    void processIdentityRoundTripsAndChecksStartTime() throws Exception {
        ECProcessIdentityStore store = new ECProcessIdentityStore(home);
        assertTrue(store.read("ec01").isEmpty());
        store.write("ec01", ProcessHandle.current());
        var identity = store.read("ec01").orElseThrow();
        assertTrue(identity.matches(ProcessHandle.current()));
        assertEquals(home.toString(), identity.applicationHome());
        assertFalse(new ECProcessIdentityStore.Identity(identity.pid(), Instant.EPOCH,
                "ec01", home.toString()).matches(ProcessHandle.current()));
        store.write("ec01", ProcessHandle.current());
        try (var files = Files.list(home.resolve("run"))) {
            assertEquals(1, files.count());
        }
    }

    @Test
    void malformedAndWrongInstallationRecordsAreRejected() throws Exception {
        ECProcessIdentityStore store = new ECProcessIdentityStore(home);
        store.write("ec01", ProcessHandle.current());
        Files.writeString(store.path("ec01"), "pid=abc\n");
        assertThrows(IOException.class, () -> store.read("ec01"));
        store.write("ec01", ProcessHandle.current());
        Files.writeString(store.path("ec01"), Files.readString(store.path("ec01"))
                .replace("containerId=ec01", "containerId=ec02"));
        assertThrows(IOException.class, () -> store.read("ec01"));
        assertThrows(IllegalArgumentException.class, () -> store.path("../ec01"));
    }
}
