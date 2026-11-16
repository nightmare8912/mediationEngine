
package org.prats.managers;

import org.prats.definitions.enums.Permission;
import org.prats.definitions.records.Session;

import org.prats.loggers.EngineLogger;
import org.prats.registries.SessionRegistry;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Connection-independent session manager.
 *
 * Provides authentication-related session operations
 * to trusted internal services.
 *
 * Responsibilities:
 *
 * 1. Register authenticated sessions.
 * 2. Resolve authoritative sessions by UUID.
 * 3. Validate existing session credentials.
 * 4. Refresh time-limited sessions.
 * 5. Evaluate permissions.
 * 6. Revoke sessions during logout.
 * 7. Trigger expired-session cleanup.
 * 8. Expose session-management metrics.
 *
 * SessionRegistry owns session storage.
 *
 * No TCP connection is retained by this manager.
 *
 * A disconnected client may reconnect and present
 * the same valid session credential.
 *
 * SECURITY:
 *
 * Session registration must follow successful
 * authentication by a trusted handler.
 *
 * Authorization must always use permissions retrieved
 * from the authoritative session registry.
 */
public final class InMemorySessionManager {

    private static final EngineLogger log =
            EngineLogger.getLogger(
                    InMemorySessionManager.class
            );

    // =========================================================
    // 1. DEPENDENCIES
    // =========================================================

    /**
     * Authoritative session storage.
     *
     * A single instance is shared by authentication
     * and authorization components.
     */
    private final SessionRegistry sessionRegistry;

    // =========================================================
    // 2. METRICS
    // =========================================================

    private final AtomicLong permissionChecks =
            new AtomicLong();

    private final AtomicLong permissionDenials =
            new AtomicLong();

    private final AtomicLong logoutRequests =
            new AtomicLong();

    private final AtomicLong successfulLogouts =
            new AtomicLong();

    private final AtomicLong cleanupCycles =
            new AtomicLong();

    // =========================================================
    // 3. CONSTRUCTORS
    // =========================================================

    /**
     * Initializes a session manager with the
     * default SessionRegistry configuration.
     */
    public InMemorySessionManager() {

        this(new SessionRegistry());
    }

    /**
     * Initializes a session manager with a
     * supplied shared SessionRegistry.
     *
     * Dependency injection allows tests and other
     * trusted components to use an explicitly
     * configured registry instance.
     */
    public InMemorySessionManager(
            SessionRegistry sessionRegistry
    ) {

        this.sessionRegistry = Objects.requireNonNull(
                sessionRegistry,
                "SessionRegistry cannot be null"
        );

        log.info(
                "[InMemorySessionManager][constructor] "
                        + "Connection-independent session manager initialized"
        );
    }

    /**
     * Initializes a session manager with a
     * custom session policy.
     */
    public InMemorySessionManager(
            int maxSessions,
            long maxLifetimeSeconds,
            boolean allowNonExpiringSessions
    ) {

        this(
                new SessionRegistry(
                        maxSessions,
                        maxLifetimeSeconds,
                        allowNonExpiringSessions
                )
        );
    }

    // =========================================================
    // 4. REGISTER SESSION
    // =========================================================

    /**
     * Creates a session for an authenticated identity.
     *
     * This operation must only be invoked after
     * AuthenticationHandler successfully verifies
     * the credentials or another trusted identity
     * verification mechanism.
     *
     * The registry enforces:
     *
     * - Session capacity.
     * - One active session per user.
     * - Expiration policy.
     * - Defensive permission copying.
     *
     * @param userId Authenticated user identifier.
     * @param permissions Verified server-side permissions.
     * @param ttlSeconds Requested session TTL.
     *
     * @return Newly created session.
     */
    public Session registerSession(
            String userId,
            Set<Permission> permissions,
            long ttlSeconds
    ) {

        log.debug(() ->
                "[InMemorySessionManager][registerSession] "
                        + "Session registration requested"
                        + " | ttlSeconds=" + ttlSeconds
        );

        Session session =
                sessionRegistry.createSession(
                        userId,
                        permissions,
                        ttlSeconds
                );

        log.info(() ->
                "[InMemorySessionManager][registerSession] "
                        + "Authenticated session registered"
                        + " | userId=" + session.username()
                        + " | expiresAt=" + session.expiresAt()
        );

        return session;
    }

    // =========================================================
    // 5. RESOLVE SESSION
    // =========================================================

    /**
     * Returns the authoritative session for a UUID.
     *
     * The result is empty when the session:
     *
     * - Does not exist.
     * - Has expired.
     * - Has been revoked.
     *
     * A session record sent by a client must never
     * replace the authoritative stored record.
     */
    public Optional<Session> getSession(
            UUID sessionId
    ) {

        /*
         * Successful lookups intentionally avoid
         * per-request DEBUG logging.
         *
         * Authorization checks may execute at
         * high frequency.
         */
        return sessionRegistry.getSession(sessionId);
    }

    // =========================================================
    // 6. VALIDATE SESSION
    // =========================================================

    /**
     * Validates the presented session credential.
     *
     * This operation does not depend on a TCP socket.
     */
    public boolean validateSession(
            UUID sessionId
    ) {

        return sessionRegistry.validateSession(sessionId);
    }

    /**
     * Compatibility validation for a Session record.
     *
     * The authoritative stored session is used for
     * validation.
     */
    public boolean validateSession(
            Session session
    ) {

        return sessionRegistry.validateSession(session);
    }

    // =========================================================
    // 7. USER SESSION EXISTENCE
    // =========================================================

    /**
     * Reports whether a user currently has an
     * active session.
     *
     * Intended for internal administrative queries.
     *
     * SECURITY:
     *
     * A username is not an authentication credential.
     * This method must never authorize requests
     * originating from external clients.
     */
    public boolean validateUserSession(
            String userId
    ) {

        return sessionRegistry.hasActiveSessionForUser(
                userId
        );
    }

    // =========================================================
    // 8. REFRESH SESSION
    // =========================================================

    /**
     * Attempts to extend a valid session.
     *
     * A refresh cannot:
     *
     * - Restore an expired session.
     * - Restore a revoked session.
     * - Exceed the absolute session lifetime.
     *
     * The session UUID remains unchanged.
     *
     * @param sessionId Existing session UUID.
     * @param ttlSeconds Requested inactivity window.
     *
     * @return Refreshed session when valid.
     */
    public Optional<Session> refreshSession(
            UUID sessionId,
            long ttlSeconds
    ) {

        log.debug(() ->
                "[InMemorySessionManager][refreshSession] "
                        + "Session refresh requested"
                        + " | ttlSeconds=" + ttlSeconds
        );

        Optional<Session> refreshed =
                sessionRegistry.refreshSession(
                        sessionId,
                        ttlSeconds
                );

        if (refreshed.isEmpty()) {

            log.debug(
                    "[InMemorySessionManager][refreshSession] "
                            + "Session refresh rejected"
            );
        }

        return refreshed;
    }

    // =========================================================
    // 9. PERMISSION VALIDATION
    // =========================================================

    /**
     * Determines whether a valid session contains
     * the requested permission.
     *
     * Permissions are retrieved from the authoritative
     * registry.
     *
     * Client-supplied permission lists are ignored.
     *
     * @param sessionId Session UUID.
     * @param permission Required permission.
     *
     * @return true when the session is valid
     *         and the permission is present.
     */
    public boolean hasPermission(
            UUID sessionId,
            Permission permission
    ) {

        Objects.requireNonNull(
                permission,
                "Permission cannot be null"
        );

        permissionChecks.incrementAndGet();

        boolean allowed =
                sessionRegistry.getSession(sessionId)
                        .map(session ->
                                session.permissions().contains(
                                        permission
                                )
                        )
                        .orElse(false);

        if (!allowed) {

            long count =
                    permissionDenials.incrementAndGet();

            /*
             * Avoid writing a warning for every rejected
             * permission check during request bursts.
             */
            if (isPowerOfTwo(count)) {

                log.warn(
                        "[InMemorySessionManager][hasPermission] "
                                + "Permission check rejected"
                                + " | requiredPermission="
                                + permission
                                + " | totalDenials=" + count
                );
            }
        }

        return allowed;
    }

    // =========================================================
    // 10. LOGOUT
    // =========================================================

    /**
     * Revokes the session identified by its UUID.
     *
     * Logout does not require a ClientConnection.
     *
     * Once revoked, the UUID is no longer accepted
     * for new session validations.
     *
     * The TCP connection may remain open and perform
     * another authentication request.
     *
     * @param sessionId Session credential to revoke.
     *
     * @return true if an existing session was revoked.
     */
    public boolean logout(
            UUID sessionId
    ) {

        logoutRequests.incrementAndGet();

        boolean revoked =
                sessionRegistry.removeSession(sessionId);

        if (revoked) {

            successfulLogouts.incrementAndGet();

            log.info(
                    "[InMemorySessionManager][logout] "
                            + "Session logout completed successfully"
            );

        } else {

            log.debug(
                    "[InMemorySessionManager][logout] "
                            + "Logout requested for missing or revoked session"
            );
        }

        return revoked;
    }

    // =========================================================
    // 11. CLEANUP EXPIRED SESSIONS
    // =========================================================

    /**
     * Removes expired session records.
     *
     * Intended for periodic invocation by an existing
     * service housekeeping scheduler.
     *
     * No TCP connections are closed.
     *
     * Non-expiring sessions remain until explicitly
     * revoked.
     */
    public void cleanupExpiredSessions() {

        cleanupCycles.incrementAndGet();

        log.debug(() ->
                "[InMemorySessionManager][cleanupExpiredSessions] "
                        + "Session cleanup started"
                        + " | storedSessions="
                        + sessionRegistry.getSize()
        );

        int removed =
                sessionRegistry.removeExpired();

        if (removed > 0) {

            log.info(() ->
                    "[InMemorySessionManager][cleanupExpiredSessions] "
                            + "Expired sessions removed"
                            + " | removedSessions=" + removed
                            + " | remainingSessions="
                            + sessionRegistry.getSize()
            );

        } else {

            log.debug(
                    "[InMemorySessionManager][cleanupExpiredSessions] "
                            + "Session cleanup completed without removals"
            );
        }
    }

    // =========================================================
    // 12. SESSION COUNTS
    // =========================================================

    /**
     * Returns the number of currently valid sessions.
     */
    public int getActiveSessionCount() {

        return Math.toIntExact(
                sessionRegistry.getActiveSessionCount()
        );
    }

    /**
     * Returns the number of stored session records.
     *
     * Expired sessions awaiting cleanup may be included.
     */
    public int getStoredSessionCount() {

        return sessionRegistry.getSize();
    }

    // =========================================================
    // 13. METRICS
    // =========================================================

    /**
     * Returns an immutable metrics snapshot.
     *
     * Combines SessionRegistry metrics with manager
     * permission, logout and cleanup counters.
     */
    public Map<String, Object> metrics() {

        Map<String, Object> metrics =
                new LinkedHashMap<>(
                        sessionRegistry.metrics()
                );

        metrics.put(
                "permissionChecks",
                permissionChecks.get()
        );

        metrics.put(
                "permissionDenials",
                permissionDenials.get()
        );

        metrics.put(
                "logoutRequests",
                logoutRequests.get()
        );

        metrics.put(
                "successfulLogouts",
                successfulLogouts.get()
        );

        metrics.put(
                "cleanupCycles",
                cleanupCycles.get()
        );

        return Map.copyOf(metrics);
    }

    // =========================================================
    // 14. INTERNAL UTILITIES
    // =========================================================

    /**
     * Returns true when a positive number
     * is an exact power of two.
     *
     * Used for rate-limited warning logs.
     */
    private static boolean isPowerOfTwo(
            long value
    ) {

        return value > 0
                && (value & (value - 1)) == 0;
    }
}
