
package org.prats.registries;

import org.prats.definitions.enums.Permission;
import org.prats.definitions.interfaces.SessionManager;
import org.prats.definitions.records.Session;

import java.time.Duration;
import java.time.Instant;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;

/**
 * In-memory session manager responsible for authentication,
 * session creation, expiration, retrieval, and logout.
 */
public class InMemorySessionManager implements SessionManager {

    private static final int MAX_SESSIONS = 10_000;

    private final ConcurrentHashMap<UUID, Session> sessions;
    private final Duration sessionDuration;

    private final BiPredicate<String, char[]> credentialVerifier;


    // =========================================================
    // 1. CONSTRUCTORS
    // =========================================================

    public InMemorySessionManager(Duration sessionDuration) {

        this(
                sessionDuration,
                (username, password) -> false
        );
    }

    public InMemorySessionManager(
            Duration sessionDuration,
            BiPredicate<String, char[]> credentialVerifier
    ) {

        this.sessions           = new ConcurrentHashMap<>();
        this.sessionDuration    = sessionDuration;

        this.credentialVerifier =
                Objects.requireNonNull(credentialVerifier);
    }


    // =========================================================
    // 2. USER LOGIN
    // =========================================================

    @Override
    public Session login(
            String username,
            char[] password
    ) {


        // -----------------------------------------------------
        // 2.1. Verify credentials
        // -----------------------------------------------------

        if (!credentialVerifier.test(username, password)) {

            throw new IllegalArgumentException(
                    "Invalid username or password"
            );
        }


        // -----------------------------------------------------
        // 2.2. Remove expired sessions
        // -----------------------------------------------------

        sessions.entrySet().removeIf(
                entry -> entry.getValue().isExpired()
        );


        // -----------------------------------------------------
        // 2.3. Enforce session capacity
        // -----------------------------------------------------

        if (sessions.size() >= MAX_SESSIONS) {

            throw new IllegalStateException(
                    "Session capacity reached"
            );
        }


        // -----------------------------------------------------
        // 2.4. Generate session identity
        // -----------------------------------------------------

        UUID sessionId = UUID.randomUUID();

        Instant now = Instant.now();


        // -----------------------------------------------------
        // 2.5. Create authenticated session
        // -----------------------------------------------------

        Session session = new Session(
                sessionId,
                username,
                Set.of(Permission.VIEW_STATUS),
                now,
                now.plus(sessionDuration)
        );


        // -----------------------------------------------------
        // 2.6. Register session
        // -----------------------------------------------------

        sessions.put(
                sessionId,
                session
        );

        return session;
    }


    // =========================================================
    // 3. ACTIVE SESSION LOOKUP
    // =========================================================

    @Override
    public Optional<Session> getActiveSession(
            UUID sessionId
    ) {

        Session session = sessions.get(sessionId);

        if (session == null) {
            return Optional.empty();
        }


        // -----------------------------------------------------
        // 3.1. Check session expiration
        // -----------------------------------------------------

        if (session.isExpired()) {

            sessions.remove(
                    sessionId,
                    session
            );

            return Optional.empty();
        }


        // -----------------------------------------------------
        // 3.2. Return active session
        // -----------------------------------------------------

        return Optional.of(session);
    }


    // =========================================================
    // 4. USER LOGOUT
    // =========================================================

    @Override
    public void logout(UUID sessionId) {

        sessions.remove(sessionId);
    }
}
