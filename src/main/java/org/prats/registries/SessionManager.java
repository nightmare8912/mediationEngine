package org.prats.registries;

import org.prats.definitions.Session;

import java.util.Optional;
import java.util.UUID;

public interface SessionManager {

    Session login(String username, char[] password);

    Optional<Session> getActiveSession(UUID sessionId);

    void logout(UUID sessionId);
}
