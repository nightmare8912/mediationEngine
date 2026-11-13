package org.prats.definitions.interfaces;

import org.prats.definitions.records.Session;

import java.util.Optional;
import java.util.UUID;

public interface SessionManager {

    Session login(String username, char[] password);

    Optional<Session> getActiveSession(UUID sessionId);

    void logout(UUID sessionId);
}
