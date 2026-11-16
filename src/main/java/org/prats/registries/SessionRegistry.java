
package org.prats.registries;

import org.prats.definitions.enums.Permission;
import org.prats.definitions.records.Session;
import org.prats.loggers.EngineLogger;

import java.time.DateTimeException;
import java.time.Instant;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe, connection-independent session registry.
 *
 * Responsibilities:
 *
 * 1. Create authenticated sessions.
 * 2. Enforce one active session per user.
 * 3. Resolve sessions using session UUIDs.
 * 4. Validate session expiration.
 * 5. Refresh valid sessions.
 * 6. Revoke exact sessions.
 * 7. Remove expired sessions.
 * 8. Expose registry metrics.
 *
 * Sessions are not associated with TCP sockets.
 *
 * Closing a TCP connection does not remove a session.
 * An authenticated client may reconnect and present
 * the same session credential until expiration or
 * explicit revocation.
 *
 * SECURITY:
 *
 * A session UUID acts as a bearer credential.
 * It must not be logged or accepted as proof of
 * identity without an authenticated, protected
 * transport and normal request authorization.
 *
 * Session creation is a privileged server-side
 * operation and must follow successful authentication.
 */
public final class SessionRegistry {

    private static final EngineLogger log =
            EngineLogger.getLogger(SessionRegistry.class);

    // =========================================================
    // 1. CONFIGURATION
    // =========================================================

    private static final int DEFAULT_MAX_SESSIONS = 30;

    private static final long DEFAULT_MAX_LIFETIME_SECONDS =
            8L * 60L * 60L;

    /**
     * Explicit sentinel for sessions with no
     * time-based expiration.
     *
     * Such sessions remain revocable.
     *
     * Non-expiring sessions require an explicit
     * configuration opt-in.
     */
    private static final Instant NEVER_EXPIRES =
            Instant.parse("9999-12-31T23:59:59.999999999Z");

    private final int maxSessions;

    private final long maxLifetimeSeconds;

    private final boolean allowNonExpiringSessions;

    // =========================================================
    // 2. SESSION STORAGE
    // =========================================================

    /**
     * Primary session registry.
     *
     * Key:
     *     Session UUID.
     *
     * Value:
     *     Authoritative Session.
     *
     * Successful lookups require only one concurrent
     * map read, without acquiring the lifecycle lock.
     */
    private final Map<UUID, Session> sessionsById =
            new ConcurrentHashMap<>();

    /**
     * Secondary index enforcing one active session
     * per normalized user ID.
     *
     * Example:
     *
     * ADMIN -> UUID
     * USER1 -> UUID
     *
     * Every access to this HashMap is protected
     * by lifecycleLock.
     */
    private final Map<String, UUID> sessionIdsByUser =
            new HashMap<>();

    /**
     * Coordinates changes to both indexes.
     *
     * Multi-map modifications must be atomic
     * relative to other lifecycle operations.
     *
     * Ordinary successful session lookups do not
     * acquire this lock.
     */
    private final Object lifecycleLock =
            new Object();

    // =========================================================
    // 3. METRICS
    // =========================================================

    private final AtomicLong createdSessions =
            new AtomicLong();

    private final AtomicLong refreshedSessions =
            new AtomicLong();

    private final AtomicLong revokedSessions =
            new AtomicLong();

    private final AtomicLong expiredSessions =
            new AtomicLong();

    private final AtomicLong rejectedRegistrations =
            new AtomicLong();

    private final AtomicLong successfulValidations =
            new AtomicLong();

    private final AtomicLong failedValidations =
            new AtomicLong();

    // =========================================================
    // 4. CONSTRUCTORS
    // =========================================================

    /**
     * Creates a registry with default policy.
     *
     * Maximum sessions: 30.
     * Maximum lifetime: 8 hours.
     * Non-expiring sessions: disabled.
     */
    public SessionRegistry() {

        this(
                DEFAULT_MAX_SESSIONS,
                DEFAULT_MAX_LIFETIME_SECONDS,
                false
        );
    }

    /**
     * Creates a registry with an explicit policy.
     *
     * @param maxSessions Maximum simultaneously stored sessions.
     * @param maxLifetimeSeconds Absolute lifetime for timed sessions.
     * @param allowNonExpiringSessions Whether TTL zero is permitted.
     */
    public SessionRegistry(
            int maxSessions,
            long maxLifetimeSeconds,
            boolean allowNonExpiringSessions
    ) {

        if (maxSessions < 1) {

            throw new IllegalArgumentException(
                    "Maximum sessions must be positive"
            );
        }

        if (maxLifetimeSeconds < 1) {

            throw new IllegalArgumentException(
                    "Maximum session lifetime must be positive"
            );
        }

        this.maxSessions = maxSessions;

        this.maxLifetimeSeconds =
                maxLifetimeSeconds;

        this.allowNonExpiringSessions =
                allowNonExpiringSessions;

        log.info(() ->
                "[SessionRegistry][constructor] "
                        + "Session registry initialized"
                        + " | maxSessions=" + maxSessions
                        + " | maxLifetimeSeconds="
                        + maxLifetimeSeconds
                        + " | allowNonExpiringSessions="
                        + allowNonExpiringSessions
        );
    }

    // =========================================================
    // 5. CREATE SESSION
    // =========================================================

    /**
     * Registers a new session after successful
     * credential or machine-identity authentication.
     *
     * Only one active session per user is permitted.
     *
     * Expired sessions can be replaced.
     *
     * TTL policy:
     *
     * ttlSeconds > 0:
     *     Create a time-limited session.
     *
     * ttlSeconds == 0:
     *     Create a non-expiring session only when
     *     the registry explicitly permits it.
     *
     * ttlSeconds < 0:
     *     Reject the request.
     *
     * A timed session cannot initially exceed
     * the configured absolute lifetime.
     *
     * @return Newly registered authoritative Session.
     */
    public Session createSession(
            String userId,
            Set<Permission> permissions,
            long ttlSeconds
    ) {

        // -----------------------------------------------------
        // VALIDATE INPUT
        // -----------------------------------------------------

        String normalizedId =
                requireNormalizedId(userId);

        Objects.requireNonNull(
                permissions,
                "Session permissions cannot be null"
        );

        if (permissions.contains(null)) {

            throw new IllegalArgumentException(
                    "Session permissions cannot contain null"
            );
        }

        if (ttlSeconds < 0) {

            throw new IllegalArgumentException(
                    "Session TTL cannot be negative"
            );
        }

        if (ttlSeconds == 0
                && !allowNonExpiringSessions) {

            throw new IllegalArgumentException(
                    "Non-expiring sessions are disabled"
            );
        }

        Set<Permission> safePermissions =
                Set.copyOf(permissions);

        // -----------------------------------------------------
        // COORDINATE REGISTRATION
        // -----------------------------------------------------

        synchronized (lifecycleLock) {

            Instant now = Instant.now();

            /*
             * Remove expired entries before checking
             * registration capacity.
             */
            removeExpiredLocked(now);

            // -------------------------------------------------
            // ENFORCE ONE SESSION PER USER
            // -------------------------------------------------

            UUID existingSessionId =
                    sessionIdsByUser.get(normalizedId);

            if (existingSessionId != null) {

                Session existing =
                        sessionsById.get(existingSessionId);

                if (existing != null
                        && !isExpired(existing, now)) {

                    long count =
                            rejectedRegistrations.incrementAndGet();

                    if (isPowerOfTwo(count)) {

                        log.warn(
                                "[SessionRegistry][createSession] "
                                        + "Active session already exists"
                                        + " | totalRejected="
                                        + count
                        );
                    }

                    throw new IllegalStateException(
                            "User already has an active session"
                    );
                }

                /*
                 * Defensive removal of an obsolete
                 * secondary index entry.
                 */
                sessionIdsByUser.remove(
                        normalizedId,
                        existingSessionId
                );
            }

            // -------------------------------------------------
            // CHECK REGISTRY CAPACITY
            // -------------------------------------------------

            if (sessionsById.size() >= maxSessions) {

                long count =
                        rejectedRegistrations.incrementAndGet();

                if (isPowerOfTwo(count)) {

                    log.warn(
                            "[SessionRegistry][createSession] "
                                    + "Session registry capacity exhausted"
                                    + " | maxSessions=" + maxSessions
                                    + " | rejectedRegistrations="
                                    + count
                    );
                }

                throw new IllegalStateException(
                        "Maximum active session limit reached"
                );
            }

            // -------------------------------------------------
            // GENERATE EXPIRATION
            // -------------------------------------------------

            Instant expiresAt;

            if (ttlSeconds == 0) {

                expiresAt = NEVER_EXPIRES;

            } else {

                long effectiveLifetime =
                        Math.min(
                                ttlSeconds,
                                maxLifetimeSeconds
                        );

                expiresAt = plusSecondsChecked(
                        now,
                        effectiveLifetime
                );
            }

            // -------------------------------------------------
            // GENERATE UNIQUE SESSION IDENTIFIER
            // -------------------------------------------------

            UUID sessionId;

            do {

                sessionId = UUID.randomUUID();

            } while (sessionsById.containsKey(sessionId));

            // -------------------------------------------------
            // CREATE IMMUTABLE SESSION
            // -------------------------------------------------

            Session session = new Session(
                    sessionId,
                    normalizedId,
                    safePermissions,
                    now,
                    expiresAt
            );

            // -------------------------------------------------
            // REGISTER SESSION
            // -------------------------------------------------

            /*
             * Publish the secondary mapping first,
             * then the authoritative session.
             *
             * A concurrent reader may observe the
             * primary session only after its user
             * index has been populated.
             */
            sessionIdsByUser.put(
                    normalizedId,
                    sessionId
            );

            sessionsById.put(
                    sessionId,
                    session
            );

            createdSessions.incrementAndGet();

            log.info(() ->
                    "[SessionRegistry][createSession] "
                            + "Session created successfully"
                            + " | userId=" + normalizedId
                            + " | permissionCount="
                            + safePermissions.size()
                            + " | expiresAt=" + expiresAt
                            + " | activeSessions="
                            + sessionsById.size()
            );

            /*
             * The UUID is deliberately excluded
             * from application logs.
             */
            return session;
        }
    }

    // =========================================================
    // 6. RESOLVE SESSION BY UUID
    // =========================================================

    /**
     * Retrieves the authoritative session associated
     * with a presented session UUID.
     *
     * A valid session must:
     *
     * 1. Exist in the primary registry.
     * 2. Have an expiration time after the current time.
     *
     * A successful lookup does not acquire lifecycleLock.
     *
     * IMPORTANT:
     *
     * This method verifies current session validity.
     * It cannot retroactively cancel privileged work
     * already admitted before a concurrent revocation.
     */
    public Optional<Session> getSession(
            UUID sessionId
    ) {

        if (sessionId == null) {

            recordFailedValidation();

            return Optional.empty();
        }

        Session session =
                sessionsById.get(sessionId);

        if (session == null) {

            recordFailedValidation();

            return Optional.empty();
        }

        Instant now = Instant.now();

        if (isExpired(session, now)) {

            /*
             * Remove the expired session and its
             * secondary index using the lifecycle lock.
             *
             * Conditional removal prevents a refreshed
             * version from being removed accidentally.
             */
            removeExpiredSession(
                    sessionId,
                    session
            );

            recordFailedValidation();

            return Optional.empty();
        }

        successfulValidations.incrementAndGet();

        return Optional.of(session);
    }

    // =========================================================
    // 7. VALIDATE SESSION
    // =========================================================

    /**
     * Validates a bearer session UUID.
     *
     * The username and permissions are resolved from
     * the authoritative registry, not from the client.
     */
    public boolean validateSession(
            UUID sessionId
    ) {

        return getSession(sessionId).isPresent();
    }

    /**
     * Compatibility overload for an existing Session.
     *
     * Only identity and current registry state
     * determine validity.
     *
     * Permissions supplied in the presented record
     * are never used for authorization.
     */
    public boolean validateSession(
            Session presentedSession
    ) {

        if (presentedSession == null) {
            return false;
        }

        return getSession(
                presentedSession.sessionId()
        )
                .map(stored ->
                        stored.username().equals(
                                normalizeId(
                                        presentedSession.username()
                                )
                        )
                )
                .orElse(false);
    }

    /**
     * Compatibility overload validating both
     * the normalized user ID and session UUID.
     *
     * The UUID remains the authentication credential.
     * A username alone is never sufficient.
     */
    public boolean validateSession(
            String userId,
            UUID sessionId
    ) {

        String normalizedId =
                normalizeId(userId);

        if (normalizedId == null) {
            return false;
        }

        return getSession(sessionId)
                .map(session ->
                        session.username().equals(normalizedId)
                )
                .orElse(false);
    }

    // =========================================================
    // 8. REFRESH SESSION
    // =========================================================

    /**
     * Extends a valid, time-limited session.
     *
     * Refresh rules:
     *
     * 1. The session must exist.
     * 2. The session must not have expired.
     * 3. The requested TTL must be positive.
     * 4. The original session UUID is preserved.
     * 5. The original creation time is preserved.
     * 6. Expiration cannot exceed absolute lifetime.
     *
     * Refresh is throttled when sufficient session
     * lifetime remains to avoid unnecessary updates
     * on every GUI click.
     *
     * A non-expiring session is returned unchanged.
     *
     * @return Current or refreshed session, if valid.
     */
    public Optional<Session> refreshSession(
            UUID sessionId,
            long ttlSeconds
    ) {

        if (sessionId == null) {
            return Optional.empty();
        }

        if (ttlSeconds <= 0) {

            throw new IllegalArgumentException(
                    "Refresh TTL must be positive"
            );
        }

        synchronized (lifecycleLock) {

            Session existing =
                    sessionsById.get(sessionId);

            if (existing == null) {
                return Optional.empty();
            }

            Instant now = Instant.now();

            // -------------------------------------------------
            // REJECT EXPIRED SESSION
            // -------------------------------------------------

            if (isExpired(existing, now)) {

                removeExpiredLocked(
                        sessionId,
                        existing,
                        now
                );

                log.debug(
                        "[SessionRegistry][refreshSession] "
                                + "Expired session cannot be refreshed"
                );

                return Optional.empty();
            }

            // -------------------------------------------------
            // HANDLE NON-EXPIRING SESSION
            // -------------------------------------------------

            if (NEVER_EXPIRES.equals(
                    existing.expiresAt()
            )) {

                return Optional.of(existing);
            }

            // -------------------------------------------------
            // CALCULATE ABSOLUTE EXPIRATION
            // -------------------------------------------------

            Instant absoluteExpiry =
                    plusSecondsChecked(
                            existing.createdAt(),
                            maxLifetimeSeconds
                    );

            // -------------------------------------------------
            // CALCULATE REQUESTED EXPIRATION
            // -------------------------------------------------

            Instant requestedExpiry =
                    plusSecondsChecked(
                            now,
                            Math.min(
                                    ttlSeconds,
                                    maxLifetimeSeconds
                            )
                    );

            Instant nextExpiry =
                    requestedExpiry.isBefore(absoluteExpiry)
                            ? requestedExpiry
                            : absoluteExpiry;

            // -------------------------------------------------
            // PREVENT EXPIRY REDUCTION
            // -------------------------------------------------

            if (!nextExpiry.isAfter(
                    existing.expiresAt()
            )) {

                return Optional.of(existing);
            }

            // -------------------------------------------------
            // AVOID REFRESHING ON EVERY REQUEST
            // -------------------------------------------------

            /*
             * Refresh only after the remaining time
             * falls below approximately half of the
             * requested inactivity window.
             *
             * Example:
             *
             * TTL = 30 minutes.
             *
             * A refresh is skipped while more than
             * 15 minutes of validity remain.
             */
            long remainingSeconds =
                    existing.expiresAt().getEpochSecond()
                            - now.getEpochSecond();

            long refreshThresholdSeconds =
                    Math.max(1, ttlSeconds / 2);

            if (remainingSeconds > refreshThresholdSeconds) {

                return Optional.of(existing);
            }

            // -------------------------------------------------
            // REPLACE IMMUTABLE SESSION RECORD
            // -------------------------------------------------

            Session refreshed = new Session(
                    existing.sessionId(),
                    existing.username(),
                    existing.permissions(),
                    existing.createdAt(),
                    nextExpiry
            );

            /*
             * The lifecycle lock protects session
             * mutations against concurrent logout,
             * cleanup and additional refresh requests.
             */
            sessionsById.put(
                    sessionId,
                    refreshed
            );

            refreshedSessions.incrementAndGet();

            log.debug(() ->
                    "[SessionRegistry][refreshSession] "
                            + "Session expiration extended"
                            + " | userId=" + existing.username()
                            + " | expiresAt=" + nextExpiry
            );

            return Optional.of(refreshed);
        }
    }

    // =========================================================
    // 9. REVOKE SESSION
    // =========================================================

    /**
     * Revokes the exact session identified by UUID.
     *
     * The operation does not depend on a TCP connection.
     *
     * Logging out invalidates the credential for all
     * future registry validations.
     */
    public boolean removeSession(
            UUID sessionId
    ) {

        if (sessionId == null) {
            return false;
        }

        synchronized (lifecycleLock) {

            Session removed =
                    sessionsById.remove(sessionId);

            if (removed == null) {

                log.debug(
                        "[SessionRegistry][removeSession] "
                                + "Session not found"
                );

                return false;
            }

            /*
             * Remove the secondary index only when
             * it still points to the revoked UUID.
             */
            sessionIdsByUser.remove(
                    removed.username(),
                    sessionId
            );

            revokedSessions.incrementAndGet();

            log.info(() ->
                    "[SessionRegistry][removeSession] "
                            + "Session revoked successfully"
                            + " | userId=" + removed.username()
                            + " | remainingSessions="
                            + sessionsById.size()
            );

            return true;
        }
    }

    /**
     * Compatibility overload for revoking a session
     * only when both username and UUID match.
     */
    public boolean removeSession(
            String userId,
            UUID expectedSessionId
    ) {

        String normalizedId =
                normalizeId(userId);

        if (normalizedId == null
                || expectedSessionId == null) {

            return false;
        }

        synchronized (lifecycleLock) {

            Session existing =
                    sessionsById.get(expectedSessionId);

            if (existing == null
                    || !normalizedId.equals(
                    existing.username()
            )) {

                return false;
            }

            return removeSession(expectedSessionId);
        }
    }

    // =========================================================
    // 10. USER SESSION EXISTENCE
    // =========================================================

    /**
     * Reports whether an active session exists
     * for a normalized user ID.
     *
     * This method is intended for administrative
     * diagnostics and session policy enforcement.
     *
     * It must not authenticate an incoming client:
     * knowing a username is not proof of identity.
     */
    public boolean hasActiveSessionForUser(
            String userId
    ) {

        String normalizedId =
                normalizeId(userId);

        if (normalizedId == null) {
            return false;
        }

        synchronized (lifecycleLock) {

            UUID sessionId =
                    sessionIdsByUser.get(normalizedId);

            if (sessionId == null) {
                return false;
            }

            Session session =
                    sessionsById.get(sessionId);

            if (session == null) {

                sessionIdsByUser.remove(
                        normalizedId,
                        sessionId
                );

                return false;
            }

            Instant now = Instant.now();

            if (isExpired(session, now)) {

                removeExpiredLocked(
                        sessionId,
                        session,
                        now
                );

                return false;
            }

            return true;
        }
    }

    // =========================================================
    // 11. CLEANUP EXPIRED SESSIONS
    // =========================================================

    /**
     * Removes expired sessions and their user indexes.
     *
     * This method can be called periodically by
     * AuthenticationService.cleanup().
     *
     * Cleanup does not disconnect TCP clients.
     *
     * @return Number of expired sessions removed.
     */
    public int removeExpired() {

        synchronized (lifecycleLock) {

            int removed =
                    removeExpiredLocked(Instant.now());

            if (removed > 0) {

                log.info(() ->
                        "[SessionRegistry][removeExpired] "
                                + "Expired session cleanup completed"
                                + " | removed=" + removed
                                + " | remainingSessions="
                                + sessionsById.size()
                );

            } else {

                log.debug(() ->
                        "[SessionRegistry][removeExpired] "
                                + "No expired sessions found"
                                + " | storedSessions="
                                + sessionsById.size()
                );
            }

            return removed;
        }
    }

    /**
     * Removes all expired sessions.
     *
     * Caller must hold lifecycleLock.
     */
    private int removeExpiredLocked(
            Instant now
    ) {

        int removed = 0;

        for (Map.Entry<UUID, Session> entry :
                sessionsById.entrySet()) {

            if (removeExpiredLocked(
                    entry.getKey(),
                    entry.getValue(),
                    now
            )) {

                removed++;
            }
        }

        return removed;
    }

    /**
     * Removes one expired session conditionally.
     *
     * Caller must hold lifecycleLock.
     */
    private boolean removeExpiredLocked(
            UUID sessionId,
            Session expectedSession,
            Instant now
    ) {

        if (!isExpired(expectedSession, now)) {
            return false;
        }

        if (!sessionsById.remove(
                sessionId,
                expectedSession
        )) {

            /*
             * A refreshed session may have replaced
             * the previously observed record.
             */
            return false;
        }

        sessionIdsByUser.remove(
                expectedSession.username(),
                sessionId
        );

        expiredSessions.incrementAndGet();

        log.debug(() ->
                "[SessionRegistry][removeExpiredLocked] "
                        + "Expired session removed"
                        + " | userId="
                        + expectedSession.username()
        );

        return true;
    }

    /**
     * Performs lazy cleanup after an expired
     * session is encountered during lookup.
     */
    private void removeExpiredSession(
            UUID sessionId,
            Session expectedSession
    ) {

        synchronized (lifecycleLock) {

            removeExpiredLocked(
                    sessionId,
                    expectedSession,
                    Instant.now()
            );
        }
    }

    // =========================================================
    // 12. SESSION COUNT
    // =========================================================

    /**
     * Returns the number of stored session records.
     *
     * Entries that expired after the last cleanup
     * may remain until lookup or periodic removal.
     */
    public int getSize() {

        return sessionsById.size();
    }

    /**
     * Counts currently valid, unexpired sessions.
     *
     * This method does not mutate registry state.
     */
    public long getActiveSessionCount() {

        Instant now = Instant.now();

        return sessionsById.values()
                .stream()
                .filter(session ->
                        !isExpired(session, now)
                )
                .count();
    }

    // =========================================================
    // 13. METRICS
    // =========================================================

    /**
     * Returns an immutable metrics snapshot.
     *
     * Concurrent counters are independently sampled.
     * The result is not a globally atomic snapshot.
     */
    public Map<String, Object> metrics() {

        Map<String, Object> metrics =
                new LinkedHashMap<>();

        metrics.put("maxSessions", maxSessions);

        metrics.put(
                "maxLifetimeSeconds",
                maxLifetimeSeconds
        );

        metrics.put(
                "allowNonExpiringSessions",
                allowNonExpiringSessions
        );

        metrics.put(
                "storedSessions",
                getSize()
        );

        metrics.put(
                "activeSessions",
                getActiveSessionCount()
        );

        metrics.put(
                "createdSessions",
                createdSessions.get()
        );

        metrics.put(
                "refreshedSessions",
                refreshedSessions.get()
        );

        metrics.put(
                "revokedSessions",
                revokedSessions.get()
        );

        metrics.put(
                "expiredSessions",
                expiredSessions.get()
        );

        metrics.put(
                "rejectedRegistrations",
                rejectedRegistrations.get()
        );

        metrics.put(
                "successfulValidations",
                successfulValidations.get()
        );

        metrics.put(
                "failedValidations",
                failedValidations.get()
        );

        return Map.copyOf(metrics);
    }

    // =========================================================
    // 14. INTERNAL VALIDATION UTILITIES
    // =========================================================

    /**
     * Normalizes user IDs consistently.
     *
     * Locale.ROOT prevents environment-specific
     * case conversion behavior.
     *
     * The account database must apply the same
     * normalization policy.
     */
    private static String normalizeId(
            String userId
    ) {

        if (userId == null || userId.isBlank()) {
            return null;
        }

        return userId.trim()
                .toUpperCase(Locale.ROOT);
    }

    private static String requireNormalizedId(
            String userId
    ) {

        String normalizedId =
                normalizeId(userId);

        if (normalizedId == null) {

            throw new IllegalArgumentException(
                    "User ID cannot be null or blank"
            );
        }

        return normalizedId;
    }

    /**
     * Determines whether the session has reached
     * its expiration instant.
     */
    private static boolean isExpired(
            Session session,
            Instant now
    ) {

        return !session.expiresAt().isAfter(now);
    }

    /**
     * Performs Instant arithmetic with a clear
     * exception when the configured duration
     * exceeds the supported date range.
     */
    private static Instant plusSecondsChecked(
            Instant instant,
            long seconds
    ) {

        try {

            return instant.plusSeconds(seconds);

        } catch (DateTimeException | ArithmeticException exception) {

            throw new IllegalArgumentException(
                    "Session duration exceeds supported time range",
                    exception
            );
        }
    }

    /**
     * Records invalid session lookups.
     *
     * Warnings are emitted at exponentially
     * increasing intervals to avoid log flooding.
     */
    private void recordFailedValidation() {

        long count =
                failedValidations.incrementAndGet();

        if (isPowerOfTwo(count)) {

            log.warn(
                    "[SessionRegistry][getSession] "
                            + "Session validation failed"
                            + " | totalFailedValidations=" + count
            );
        }
    }

    private static boolean isPowerOfTwo(
            long value
    ) {

        return value > 0
                && (value & (value - 1)) == 0;
    }
}
