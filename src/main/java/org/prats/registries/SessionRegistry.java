
package org.prats.registries;

import org.prats.definitions.enums.Permission;
import org.prats.definitions.records.Session;
import org.prats.loggers.EngineLogger;

import java.time.DateTimeException;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class SessionRegistry {

    private static final EngineLogger log =
            EngineLogger.getLogger(SessionRegistry.class);

    // ---------------------------------------------------------
    // CONSTANTS
    // ---------------------------------------------------------

    /*
     * A TTL of zero represents a session without a
     * practical time-based expiry.
     *
     * The session is still explicitly revocable.
     */
    private static final Instant NEVER_EXPIRES =
            Instant.parse("9999-12-31T23:59:59.999999999Z");

    // ---------------------------------------------------------
    // SESSION STORAGE
    // ---------------------------------------------------------

    /*
     * One active session per normalized user ID.
     *
     * Example:
     *
     * ADMIN -> Session
     * USER1 -> Session
     * EC01  -> Session
     *
     * The session UUID is unique to each authentication
     * lifecycle, even when a user logs in repeatedly.
     */
    private final Map<String, Session> activeSessions =
            new ConcurrentHashMap<>();

    // ---------------------------------------------------------
    // CREATE SESSION
    // ---------------------------------------------------------

    public Session createSession(
            String userId,
            Set<Permission> permissions,
            long ttlSeconds
    ) {

        String normalizedId = requireNormalizedId(userId);

        Objects.requireNonNull(
                permissions,
                "Session permissions cannot be null"
        );

        if (permissions.stream().anyMatch(Objects::isNull)) {

            log.warn(() ->
                    "[SessionRegistry][createSession] "
                            + "Session creation rejected: null permission"
                            + " | userId=" + normalizedId
            );

            throw new IllegalArgumentException(
                    "Session permissions cannot contain null"
            );
        }

        if (ttlSeconds < 0) {

            log.warn(() ->
                    "[SessionRegistry][createSession] "
                            + "Session creation rejected: negative TTL"
                            + " | userId=" + normalizedId
                            + " | ttlSeconds=" + ttlSeconds
            );

            throw new IllegalArgumentException(
                    "Session TTL cannot be negative"
            );
        }

        Instant now = Instant.now();
        Instant expiresAt;

        try {

            expiresAt = ttlSeconds == 0
                    ? NEVER_EXPIRES
                    : now.plusSeconds(ttlSeconds);

        } catch (DateTimeException | ArithmeticException exception) {

            log.error(
                    "[SessionRegistry][createSession] "
                            + "Unable to calculate session expiry"
                            + " | userId=" + normalizedId,
                    exception
            );

            throw new IllegalArgumentException(
                    "Session TTL exceeds supported time range",
                    exception
            );
        }

        /*
         * Create a defensive copy of permissions.
         *
         * Session must not retain a mutable Set
         * controlled by the caller.
         */
        Set<Permission> safePermissions =
                Set.copyOf(permissions);

        Session newSession = new Session(
                UUID.randomUUID(),
                normalizedId,
                safePermissions,
                now,
                expiresAt
        );

        // -------------------------------------------------
        // ATOMIC REGISTRATION
        // -------------------------------------------------

        /*
         * ConcurrentHashMap operations are individually
         * atomic, but get() followed by put() is not.
         *
         * This retry loop guarantees:
         *
         * 1. Only one active session per user.
         * 2. An expired session can be replaced.
         * 3. Concurrent logins cannot overwrite each other.
         *
         * No class-wide synchronized method is needed.
         */
        while (true) {

            Session existing =
                    activeSessions.putIfAbsent(
                            normalizedId,
                            newSession
                    );

            if (existing == null) {
                break;
            }

            if (!existing.isExpired()) {

                log.warn(() ->
                        "[SessionRegistry][createSession] "
                                + "Active session already exists"
                                + " | userId=" + normalizedId
                );

                throw new IllegalStateException(
                        "User already has an active session"
                );
            }

            /*
             * Replace only the expired session we observed.
             *
             * If another thread changed the entry first,
             * replace() fails and the loop retries.
             */
            if (activeSessions.replace(
                    normalizedId,
                    existing,
                    newSession
            )) {

                log.debug(() ->
                        "[SessionRegistry][createSession] "
                                + "Expired session replaced"
                                + " | userId=" + normalizedId
                );

                break;
            }
        }

        log.info(() ->
                "[SessionRegistry][createSession] "
                        + "Session created successfully"
                        + " | userId=" + normalizedId
                        + " | permissionCount=" + safePermissions.size()
                        + " | createdAt=" + now
                        + " | expiresAt=" + expiresAt
                        + " | activeSessions=" + activeSessions.size()
        );

        return newSession;
    }

    // ---------------------------------------------------------
    // GET SESSION
    // ---------------------------------------------------------

    /*
     * Retrieve a session only if it exists
     * and has not expired.
     *
     * Expired entries are removed lazily during lookup,
     * in addition to periodic cleanup.
     */
    public Optional<Session> getSession(
            String userId
    ) {

        String normalizedId = normalizeId(userId);

        if (normalizedId == null) {

            log.debug(
                    "[SessionRegistry][getSession] "
                            + "Invalid user ID supplied"
            );

            return Optional.empty();
        }

        Session session =
                activeSessions.get(normalizedId);

        if (session == null) {
            return Optional.empty();
        }

        if (session.isExpired()) {

            boolean removed =
                    activeSessions.remove(
                            normalizedId,
                            session
                    );

            if (removed) {

                log.debug(() ->
                        "[SessionRegistry][getSession] "
                                + "Expired session removed"
                                + " | userId=" + normalizedId
                );
            }

            return Optional.empty();
        }

        return Optional.of(session);
    }

    // ---------------------------------------------------------
    // VALIDATE SESSION
    // ---------------------------------------------------------

    /*
     * Validate the presented session against the
     * authoritative session stored in the registry.
     *
     * Never trust permissions contained in a Session
     * object received from a remote client.
     */
    public boolean validateSession(
            Session presentedSession
    ) {

        if (presentedSession == null) {
            return false;
        }

        return validateSession(
                presentedSession.username(),
                presentedSession.sessionId()
        );
    }

    /*
     * A valid session must satisfy BOTH:
     *
     * 1. The user has a non-expired session.
     * 2. The supplied session UUID matches the stored one.
     */
    public boolean validateSession(
            String userId,
            UUID sessionId
    ) {

        if (sessionId == null) {
            return false;
        }

        return getSession(userId)
                .map(session ->
                        session.sessionId().equals(sessionId)
                )
                .orElse(false);
    }

    // ---------------------------------------------------------
    // REVOKE EXACT SESSION
    // ---------------------------------------------------------

    /*
     * Removes only the session with the expected UUID.
     *
     * This is important for connection cleanup:
     *
     * An old connection must not accidentally remove
     * a newer session belonging to the same user.
     */
    public boolean removeSession(
            String userId,
            UUID expectedSessionId
    ) {

        String normalizedId = normalizeId(userId);

        if (normalizedId == null ||
                expectedSessionId == null) {

            return false;
        }

        Session existing =
                activeSessions.get(normalizedId);

        if (existing == null ||
                !expectedSessionId.equals(existing.sessionId())) {

            log.debug(() ->
                    "[SessionRegistry][removeSession] "
                            + "Session not found or UUID mismatch"
                            + " | userId=" + normalizedId
            );

            return false;
        }

        /*
         * Conditional removal protects against a
         * replacement occurring between get() and remove().
         */
        boolean removed =
                activeSessions.remove(
                        normalizedId,
                        existing
                );

        if (removed) {

            log.info(() ->
                    "[SessionRegistry][removeSession] "
                            + "Session revoked"
                            + " | userId=" + normalizedId
                            + " | remainingSessions="
                            + activeSessions.size()
            );
        }

        return removed;
    }

    // ---------------------------------------------------------
    // REMOVE EXPIRED SESSIONS
    // ---------------------------------------------------------

    /*
     * Periodic cleanup job.
     *
     * Iteration over ConcurrentHashMap is weakly
     * consistent and safe during concurrent updates.
     *
     * remove(key, value) prevents a newer session
     * from being deleted by an old cleanup operation.
     */
    public int removeExpired() {

        log.debug(() ->
                "[SessionRegistry][removeExpired] "
                        + "Starting expired session cleanup"
                        + " | activeSessions=" + activeSessions.size()
        );

        int removedCount = 0;

        for (Map.Entry<String, Session> entry :
                activeSessions.entrySet()) {

            String userId = entry.getKey();
            Session session = entry.getValue();

            if (!session.isExpired()) {
                continue;
            }

            if (activeSessions.remove(userId, session)) {

                removedCount++;

                log.debug(() ->
                        "[SessionRegistry][removeExpired] "
                                + "Expired session removed"
                                + " | userId=" + userId
                                + " | expiresAt=" + session.expiresAt()
                );
            }
        }

        final int finalRemovedCount = removedCount;

        log.debug(() ->
                "[SessionRegistry][removeExpired] "
                        + "Expired session cleanup completed"
                        + " | removedSessions=" + finalRemovedCount
                        + " | remainingSessions="
                        + activeSessions.size()
        );

        return removedCount;
    }

    // ---------------------------------------------------------
    // REGISTRY SIZE
    // ---------------------------------------------------------

    public int getSize() {

        return activeSessions.size();
    }

    // ---------------------------------------------------------
    // NORMALIZE USER ID
    // ---------------------------------------------------------

    /*
     * Locale.ROOT prevents locale-specific casing
     * differences, such as Turkish i/I conversion.
     *
     * Use the same normalization policy in your
     * authentication/account service.
     */
    private static String normalizeId(
            String userId
    ) {

        if (userId == null || userId.isBlank()) {
            return null;
        }

        return userId.trim().toUpperCase(Locale.ROOT);
    }

    private static String requireNormalizedId(
            String userId
    ) {

        String normalizedId = normalizeId(userId);

        if (normalizedId == null) {

            throw new IllegalArgumentException(
                    "User ID cannot be null or blank"
            );
        }

        return normalizedId;
    }
}
