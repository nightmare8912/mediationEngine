
package org.prats.managers;

import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.enums.Permission;
import org.prats.definitions.records.Session;
import org.prats.loggers.EngineLogger;
import org.prats.registries.SessionRegistry;

import java.net.Socket;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemorySessionManager {

    private static final EngineLogger log =
            EngineLogger.getLogger(InMemorySessionManager.class);

    // ---------------------------------------------------------
    // DEPENDENCIES
    // ---------------------------------------------------------

    private final SessionRegistry sessionRegistry;

    // ---------------------------------------------------------
    // CONNECTION-BOUND SESSION REGISTRY
    // ---------------------------------------------------------

    /*
     * A binding connects an authenticated TCP socket
     * to the exact session issued for that connection.
     *
     * We store the session ID rather than relying on
     * the user ID alone.
     *
     * This prevents a stale socket from revoking
     * another login belonging to the same user.
     */
    private record SessionBinding(
            String userId,
            UUID sessionId
    ) {
    }

    private final Map<ClientConnection, SessionBinding>
            connectionSessions = new ConcurrentHashMap<>();

    /*
     * Only registration, logout and connection cleanup
     * need to coordinate their multi-step operations.
     *
     * High-frequency session validation remains lock-free.
     *
     * This monitor belongs to this manager instance.
     */
    private final Object lifecycleLock = new Object();

    // ---------------------------------------------------------
    // CONSTRUCTOR
    // ---------------------------------------------------------

    public InMemorySessionManager() {

        this.sessionRegistry = new SessionRegistry();

        log.info(
                "[InMemorySessionManager][constructor] "
                        + "In-memory session manager initialized"
        );
    }

    // ---------------------------------------------------------
    // REGISTER SESSION
    // ---------------------------------------------------------

    /*
     * Creates an authenticated user session and binds
     * it to the current TCP connection.
     *
     * IMPORTANT:
     *
     * This method must ONLY be called after
     * AuthenticationService has verified the user's
     * credentials or another trusted authentication method.
     *
     * Calling this method is a privileged server action.
     * A username supplied by a remote client is NOT
     * sufficient evidence to create a session.
     *
     * One user may have only one active session.
     */
    public Session registerSession(
            ClientConnection connection,
            String userId,
            Set<Permission> permissions,
            long ttlSeconds
    ) {

        Objects.requireNonNull(
                connection,
                "Client connection cannot be null"
        );

        // -------------------------------------------------
        // COORDINATE SESSION REGISTRATION AND BINDING
        // -------------------------------------------------

        synchronized (lifecycleLock) {

            Socket socket = connection.getSocket();

            if (socket == null ||
                    !socket.isConnected() ||
                    socket.isClosed() ||
                    socket.isInputShutdown() ||
                    socket.isOutputShutdown()) {

                log.warn(
                        "[InMemorySessionManager][registerSession] "
                                + "Session registration rejected: connection unavailable"
                );

                throw new IllegalStateException(
                        "Cannot register a session on a closed connection"
                );
            }

            SessionBinding existingBinding =
                    connectionSessions.get(connection);

            if (existingBinding != null) {

                /*
                 * Do not overwrite a valid session
                 * already bound to this connection.
                 */
                if (sessionRegistry.validateSession(
                        existingBinding.userId(),
                        existingBinding.sessionId()
                )) {

                    log.warn(
                            "[InMemorySessionManager][registerSession] "
                                    + "Connection already has an authenticated session"
                    );

                    throw new IllegalStateException(
                            "Connection already has an active session"
                    );
                }

                /*
                 * If the old session expired, discard
                 * its stale connection binding.
                 *
                 * The same TCP connection may then
                 * authenticate again.
                 */
                connectionSessions.remove(
                        connection,
                        existingBinding
                );

                log.debug(
                        "[InMemorySessionManager][registerSession] "
                                + "Stale connection binding removed"
                );
            }

            /*
             * SessionRegistry atomically enforces
             * the one-active-session-per-user rule.
             *
             * A duplicate login throws IllegalStateException.
             */
            Session session =
                    sessionRegistry.createSession(
                            userId,
                            permissions,
                            ttlSeconds
                    );

            SessionBinding binding =
                    new SessionBinding(
                            session.username(),
                            session.sessionId()
                    );

            connectionSessions.put(
                    connection,
                    binding
            );

            log.info(() ->
                    "[InMemorySessionManager][registerSession] "
                            + "Session registered and bound to TCP connection"
                            + " | userId=" + session.username()
                            + " | expiresAt=" + session.expiresAt()
                            + " | activeSessions="
                            + sessionRegistry.getSize()
                            + " | boundConnections="
                            + connectionSessions.size()
            );

            return session;
        }
    }

    // ---------------------------------------------------------
    // VALIDATE SESSION
    // ---------------------------------------------------------

    /*
     * Confirms that a Session object refers to a
     * currently stored and non-expired session.
     *
     * IMPORTANT:
     *
     * This checks session validity only.
     *
     * It does not verify ownership of a particular
     * TCP connection.
     *
     * For incoming client requests, prefer
     * validateSessionForConnection().
     */
    public boolean validateSession(
            Session session
    ) {

        if (session == null) {

            log.debug(
                    "[InMemorySessionManager][validateSession] "
                            + "Null session rejected"
            );

            return false;
        }

        boolean valid =
                sessionRegistry.validateSession(session);

        log.debug(() ->
                "[InMemorySessionManager][validateSession] "
                        + "Session validation completed"
                        + " | userId=" + session.username()
                        + " | valid=" + valid
        );

        return valid;
    }

    // ---------------------------------------------------------
    // VALIDATE USER SESSION
    // ---------------------------------------------------------

    /*
     * Checks whether the specified user currently
     * has an active, non-expired session.
     *
     * This is useful for internal session queries.
     *
     * It must NOT be used alone to authenticate
     * an incoming client request, because merely
     * knowing a username proves nothing.
     */
    public boolean validateUserSession(
            String userId
    ) {

        boolean valid =
                sessionRegistry.getSession(userId).isPresent();

        log.debug(() ->
                "[InMemorySessionManager][validateUserSession] "
                        + "User session lookup completed"
                        + " | userIdPresent="
                        + (userId != null && !userId.isBlank())
                        + " | valid=" + valid
        );

        return valid;
    }

    // ---------------------------------------------------------
    // VALIDATE CONNECTION-BOUND SESSION
    // ---------------------------------------------------------

    /*
     * The request must satisfy all conditions:
     *
     * 1. The TCP connection has a session binding.
     * 2. The supplied session UUID matches that binding.
     * 3. The authoritative stored session still exists.
     * 4. The authoritative session has not expired.
     *
     * Even if a user knows another user's session ID,
     * it will not validate against their own connection.
     */
    public boolean validateSessionForConnection(
            ClientConnection connection,
            UUID sessionId
    ) {

        return getSessionForConnection(
                connection,
                sessionId
        ).isPresent();
    }

    // ---------------------------------------------------------
    // GET AUTHENTICATED SESSION FOR CONNECTION
    // ---------------------------------------------------------

    /*
     * Returns the authoritative Session from the
     * registry, not a potentially forged Session
     * received from the network.
     *
     * Application handlers should use this returned
     * session when checking permissions.
     */
    public Optional<Session> getSessionForConnection(
            ClientConnection connection,
            UUID sessionId
    ) {

        if (connection == null || sessionId == null) {

            log.debug(
                    "[InMemorySessionManager][getSessionForConnection] "
                            + "Invalid validation arguments"
            );

            return Optional.empty();
        }

        SessionBinding binding =
                connectionSessions.get(connection);

        if (binding == null) {

            log.debug(
                    "[InMemorySessionManager][getSessionForConnection] "
                            + "Connection has no session binding"
            );

            return Optional.empty();
        }

        /*
         * The session provided by the caller must
         * be the session issued for this very socket.
         */
        if (!binding.sessionId().equals(sessionId)) {

            log.warn(
                    "[InMemorySessionManager][getSessionForConnection] "
                            + "Session ID does not match connection binding"
            );

            return Optional.empty();
        }

        /*
         * Read the authoritative session, checking
         * current expiry and the stored UUID.
         */
        Optional<Session> stored =
                sessionRegistry.getSession(
                        binding.userId()
                );

        if (stored.isEmpty()) {

            log.debug(
                    "[InMemorySessionManager][getSessionForConnection] "
                            + "Bound session is missing or expired"
            );

            return Optional.empty();
        }

        Session session = stored.get();

        if (!session.sessionId().equals(binding.sessionId())) {

            log.warn(
                    "[InMemorySessionManager][getSessionForConnection] "
                            + "Connection binding refers to a different session"
            );

            return Optional.empty();
        }

        return Optional.of(session);
    }

    // ---------------------------------------------------------
    // PERMISSION CHECK
    // ---------------------------------------------------------

    /*
     * Checks a permission against the authoritative
     * server-side session.
     *
     * Never authorize based on a permissions list
     * received in a client request.
     */
    public boolean hasPermission(
            ClientConnection connection,
            UUID sessionId,
            Permission permission
    ) {

        Objects.requireNonNull(
                permission,
                "Permission cannot be null"
        );

        boolean allowed =
                getSessionForConnection(
                        connection,
                        sessionId
                )
                        .map(session ->
                                session.permissions().contains(permission)
                        )
                        .orElse(false);

        log.debug(() ->
                "[InMemorySessionManager][hasPermission] "
                        + "Permission evaluated"
                        + " | permission=" + permission
                        + " | allowed=" + allowed
        );

        return allowed;
    }

    // ---------------------------------------------------------
    // REMOVE SESSION ON LOGOUT
    // ---------------------------------------------------------

    /*
     * A user may explicitly log out without closing
     * their TCP connection.
     *
     * The connection stays open but becomes
     * unauthenticated until another successful login.
     */
    public boolean logout(
            ClientConnection connection
    ) {

        if (connection == null) {
            return false;
        }

        synchronized (lifecycleLock) {

            SessionBinding binding =
                    connectionSessions.remove(connection);

            if (binding == null) {

                log.debug(
                        "[InMemorySessionManager][logout] "
                                + "No session bound to connection"
                );

                return false;
            }

            boolean revoked =
                    sessionRegistry.removeSession(
                            binding.userId(),
                            binding.sessionId()
                    );

            log.info(() ->
                    "[InMemorySessionManager][logout] "
                            + "Connection session invalidated"
                            + " | userId=" + binding.userId()
                            + " | revoked=" + revoked
                            + " | activeSessions="
                            + sessionRegistry.getSize()
            );

            return revoked;
        }
    }

    // ---------------------------------------------------------
    // TCP CONNECTION CLOSED
    // ---------------------------------------------------------

    /*
     * Called by TCPService or TCPProcessor when a
     * client connection closes.
     *
     * This must run for:
     *
     * - Client disconnect
     * - Read failure
     * - Write failure
     * - Forced disconnect
     * - Service shutdown
     *
     * We revoke ONLY the session bound to this
     * specific connection.
     *
     * An older socket cannot accidentally remove
     * a newer session belonging to the same user.
     */
    public void onConnectionClosed(
            ClientConnection connection
    ) {

        if (connection == null) {
            return;
        }

        log.debug(
                "[InMemorySessionManager][onConnectionClosed] "
                        + "TCP connection closure received"
        );

        synchronized (lifecycleLock) {

            SessionBinding binding =
                    connectionSessions.remove(connection);

            if (binding == null) {

                log.debug(
                        "[InMemorySessionManager][onConnectionClosed] "
                                + "No authenticated session attached to closing connection"
                );

                return;
            }

            boolean revoked =
                    sessionRegistry.removeSession(
                            binding.userId(),
                            binding.sessionId()
                    );

            log.info(() ->
                    "[InMemorySessionManager][onConnectionClosed] "
                            + "Connection-bound session cleanup completed"
                            + " | userId=" + binding.userId()
                            + " | revoked=" + revoked
                            + " | remainingSessions="
                            + sessionRegistry.getSize()
                            + " | remainingBindings="
                            + connectionSessions.size()
            );
        }
    }

    // ---------------------------------------------------------
    // CLEANUP EXPIRED SESSIONS
    // ---------------------------------------------------------

    /*
     * Invoked periodically by your existing
     * Platform/EC cleanup scheduler.
     *
     * First remove expired sessions from the registry.
     *
     * Then remove connection bindings that refer
     * to sessions which are no longer valid.
     *
     * The TCP sockets are not forcibly closed here.
     * They may remain connected and authenticate again.
     */
    public void cleanupExpiredSessions() {

        log.debug(() ->
                "[InMemorySessionManager][cleanupExpiredSessions] "
                        + "Session cleanup started"
                        + " | activeSessions="
                        + sessionRegistry.getSize()
                        + " | boundConnections="
                        + connectionSessions.size()
        );

        synchronized (lifecycleLock) {

            int expiredSessions =
                    sessionRegistry.removeExpired();

            int removedBindings = 0;

            for (Map.Entry<ClientConnection, SessionBinding> entry :
                    connectionSessions.entrySet()) {

                ClientConnection connection = entry.getKey();

                SessionBinding binding = entry.getValue();

                boolean sessionValid =
                        sessionRegistry.validateSession(
                                binding.userId(),
                                binding.sessionId()
                        );

                if (sessionValid) {
                    continue;
                }

                if (connectionSessions.remove(
                        connection,
                        binding
                )) {

                    removedBindings++;

                    log.debug(() ->
                            "[InMemorySessionManager][cleanupExpiredSessions] "
                                    + "Stale session binding removed"
                                    + " | userId=" + binding.userId()
                    );
                }
            }

            final int finalRemovedBindings = removedBindings;

            log.info(() ->
                    "[InMemorySessionManager][cleanupExpiredSessions] "
                            + "Session cleanup completed"
                            + " | expiredSessions=" + expiredSessions
                            + " | removedBindings=" + finalRemovedBindings
                            + " | activeSessions="
                            + sessionRegistry.getSize()
                            + " | boundConnections="
                            + connectionSessions.size()
            );
        }
    }

    // ---------------------------------------------------------
    // MONITORING
    // ---------------------------------------------------------

    public int getActiveSessionCount() {

        return sessionRegistry.getSize();
    }

    public int getBoundConnectionCount() {

        return connectionSessions.size();
    }
}
