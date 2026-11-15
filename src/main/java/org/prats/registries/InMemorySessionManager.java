package org.prats.registries;

import org.prats.definitions.enums.Permission;
import org.prats.definitions.interfaces.SessionManager;
import org.prats.definitions.records.Session;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class InMemorySessionManager implements SessionManager {

    private final ConcurrentHashMap<UUID, Session> sessions;
    private final Duration sessionDuration;
    private final java.util.function.BiPredicate<String, char[]> credentialVerifier;

    public InMemorySessionManager(Duration sessionDuration) {
        this(sessionDuration, (username, password) -> false);
    }

    public InMemorySessionManager(Duration sessionDuration, java.util.function.BiPredicate<String, char[]> credentialVerifier) {
        this.sessions = new ConcurrentHashMap<>();
        this.sessionDuration = sessionDuration;
        this.credentialVerifier = java.util.Objects.requireNonNull(credentialVerifier);
    }

    @Override
    public Session login(String username, char[] password) {

        if (!credentialVerifier.test(username, password)) throw new IllegalArgumentException("Invalid username or password");
        sessions.entrySet().removeIf(entry -> entry.getValue().isExpired());
        if (sessions.size() >= 10000) throw new IllegalStateException("Session capacity reached");

        UUID sessionId = UUID.randomUUID();
        Instant now = Instant.now();

        Session session = new Session(
                sessionId,
                username,
                Set.of(Permission.VIEW_STATUS),
                now,
                now.plus(sessionDuration)
        );

        sessions.put(sessionId, session);

        return session;
    }

    @Override
    public Optional<Session> getActiveSession(UUID sessionId) {
        Session session = sessions.get(sessionId);

        if (session == null) {
            return Optional.empty();
        }

        if (session.isExpired()) {
            sessions.remove(sessionId, session);
            return Optional.empty();
        }

        return Optional.of(session);
    }

    @Override
    public void logout(UUID sessionId) {
        sessions.remove(sessionId);
    }
}
