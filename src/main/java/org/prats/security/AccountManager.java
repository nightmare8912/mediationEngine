package org.prats.security;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.*;
import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.parsers.MessageParser;
import org.prats.replyChannels.FutureReplyChannel;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.prats.loggers.EngineLogger;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.security.MessageDigest;
import java.security.SecureRandom;


import java.time.Instant;

import java.util.Base64;
import java.util.Map;
import java.util.Optional;

import java.util.concurrent.ConcurrentHashMap;

/** Authentication policy and session state used only by AuthenticationHandler.
 * Storage operations use structured database messages; no JDBC is exposed here.
 */
public final class AccountManager {

    private static final EngineLogger log =
            EngineLogger.getLogger(AccountManager.class);

    private static final int ITERATIONS = 600_000;

    private final MessageDispatcher dispatcher;
    private final MessageParser parser;

    private final Map<String, AccountSession> sessions =
            new ConcurrentHashMap<>();

    private final Map<String, Attempts> attempts =
            new ConcurrentHashMap<>();



    public record AccountSession(
            String token,
            String username,
            Instant expiresAt
    ) {
    }

    private record Attempts(
            int failures,
            Instant resetAt
    ) {
    }

    public AccountManager(MessageDispatcher dispatcher, MessageParser parser) {
        this.dispatcher = java.util.Objects.requireNonNull(dispatcher);
        this.parser = java.util.Objects.requireNonNull(parser);
    }

    private CompletableFuture<JsonNode> query(Operation operation, Map<String, ?> payload) {
        Message request = parser.createEmptyMessage();
        request.setMessageType(MessageType.REQUEST);
        request.setOperation(operation);
        request.setAccountDatabaseRequest(true);
        request.setPayload(parser.convertToJsonNode(payload));
        CompletableFuture<Message> result = new CompletableFuture<>();
        try { dispatcher.dispatchRequest(request, new FutureReplyChannel(result)); }
        catch (RuntimeException error) { result.completeExceptionally(error); }
        return result.orTimeout(15, TimeUnit.SECONDS).thenApply(response -> {
            JsonNode body = response.getPayload();
            if (body == null || !body.path("ok").asBoolean(false)) {
                if (body != null && "DUPLICATE_KEY".equals(body.path("errorCode").asText()))
                    throw new IllegalArgumentException("That username is unavailable.");
                throw new IllegalStateException("Account storage is unavailable.");
            }
            return body;
        });
    }

    /**
     * Registers a new account with a securely derived password hash.
     */
    public CompletableFuture<Void> register(
            String username,
            char[] password
    ) {

        log.debug(() ->
                "[AccountManager][register] Account registration requested"
        );

        validateUsername(username);

        if (password == null
                || password.length < 10
                || password.length > 128) {

            log.debug(() ->
                    "[AccountManager][register] Account registration rejected"
                            + " | reason=INVALID_PASSWORD_LENGTH"
            );

            throw new IllegalArgumentException(
                    "Use a password between 10 and 128 characters."
            );
        }


        log.debug(() ->
                "[AccountManager][register] Generating password hash"
                        + " | algorithm=PBKDF2WithHmacSHA256"
                        + " | iterations=" + ITERATIONS
        );

        String hash = hash(password);

        log.debug(() ->
                "[AccountManager][register] Password hash generated successfully"
        );

        return query(Operation.CREATE, Map.of("entity", "users", "data",
                Map.of("user_name", username, "password", hash, "active", "Y")))
                .thenApply(ignored -> null);
    }

    /**
     * Authenticates an account and creates an expiring session.
     *
     * Includes:
     * - Credential validation.
     * - Login attempt rate limiting.
     * - Database credential lookup.
     * - PBKDF2 password verification.
     * - Session capacity enforcement.
     * - Secure session token generation.
     */
    public CompletableFuture<AccountSession> login(
            String username,
            char[] password
    ) {

        log.debug(() ->
                "[AccountManager][login] Account login requested"
        );

        validateUsername(username);

        if (password == null || password.length > 128) {

            log.debug(() ->
                    "[AccountManager][login] Login rejected"
                            + " | reason=INVALID_CREDENTIAL_FORMAT"
            );

            throw new IllegalArgumentException(
                    "Invalid username or password."
            );
        }


        /*
         * Check recent failed login attempts.
         */
        Attempts recent = attempts.get(username);

        if (recent != null
                && Instant.now().isBefore(recent.resetAt())
                && recent.failures() >= 5) {

            log.warn(
                    "[AccountManager][login] Login temporarily blocked"
                            + " | reason=TOO_MANY_FAILED_ATTEMPTS"
            );

            throw new IllegalArgumentException(
                    "Too many failed attempts. Try again in a minute."
            );
        }

        /*
         * Retrieve stored credentials and account status.
         */
        log.debug(() ->
                "[AccountManager][login] Retrieving account credentials"
        );

        return query(Operation.SEARCH, Map.of("entity", "users", "columns", List.of("password", "active"),
                "filters", Map.of("user_name", Map.of("operator", "EQ", "value", username))))
                .thenApply(response -> {
                    JsonNode row = response.path("rows").path(0);
                    String encoded = field(row, "password");
                    String active = field(row, "active");
                    return authenticate(username, password, encoded, active);
                });
    }

    private static String field(JsonNode row, String name) {
        JsonNode value = row.get(name);
        if (value == null) value = row.get(name.toUpperCase(java.util.Locale.ROOT));
        return value == null || value.isNull() ? null : value.asText();
    }

    private AccountSession authenticate(String username, char[] password, String encoded, String active) {
        /*
         * Verify credentials.
         *
         * Perform PBKDF2 work even when the account does not exist.
         * Legacy plaintext passwords are never accepted.
         */
        log.debug(() ->
                "[AccountManager][login] Starting password verification"
                        + " | algorithm=PBKDF2WithHmacSHA256"
        );

        boolean valid =
                encoded == null
                        ? verify(password, dummyHash())
                        : verify(password, encoded);

        /*
         * Handle incorrect credentials or disabled accounts.
         */
        if (!valid || !"Y".equalsIgnoreCase(active)) {

            log.debug(() ->
                    "[AccountManager][login] Authentication unsuccessful"
                            + " | reason=CREDENTIALS_OR_ACCOUNT_STATUS_INVALID"
            );

            /*
             * Remove expired attempt records when the
             * registry grows beyond its configured threshold.
             */
            if (attempts.size() > 5000) {

                log.debug(() ->
                        "[AccountManager][login] Cleaning expired login attempt records"
                                + " | currentAttempts=" + attempts.size()
                );

                attempts.entrySet().removeIf(
                        entry -> Instant.now().isAfter(
                                entry.getValue().resetAt()
                        )
                );

                log.debug(() ->
                        "[AccountManager][login] Login attempt cleanup completed"
                                + " | remainingAttempts=" + attempts.size()
                );
            }

            /*
             * Prevent additional unique usernames from being
             * tracked once the attempt registry is at capacity.
             */
            if (attempts.size() >= 5000
                    && !attempts.containsKey(username)) {

                log.warn(
                        "[AccountManager][login] Login rejected"
                                + " | reason=ATTEMPT_REGISTRY_CAPACITY_REACHED"
                );

                throw new IllegalArgumentException(
                        "Too many login attempts. Try again later."
                );
            }

            /*
             * Record the failed authentication attempt.
             */
            attempts.compute(
                    username,
                    (key, old) ->
                            old == null
                                    || Instant.now().isAfter(old.resetAt())
                                    ? new Attempts(
                                    1,
                                    Instant.now().plusSeconds(60)
                            )
                                    : new Attempts(
                                    old.failures() + 1,
                                    old.resetAt()
                            )
            );

            log.debug(() ->
                    "[AccountManager][login] Failed authentication attempt recorded"
            );

            log.warn(
                    "[AccountManager][login] Login rejected"
                            + " | username=" + username
            );

            throw new IllegalArgumentException(
                    "Invalid username or password."
            );
        }

        /*
         * Authentication succeeded.
         *
         * Clear previous failed login attempts.
         */
        attempts.remove(username);

        log.debug(() ->
                "[AccountManager][login] Credentials verified successfully"
                        + " | previousAttemptsCleared=true"
        );

        /*
         * Remove expired sessions before creating a new one.
         */
        log.debug(() ->
                "[AccountManager][login] Cleaning expired sessions"
                        + " | currentSessions=" + sessions.size()
        );

        sessions.entrySet().removeIf(
                entry -> Instant.now().isAfter(
                        entry.getValue().expiresAt()
                )
        );

        log.debug(() ->
                "[AccountManager][login] Expired session cleanup completed"
                        + " | remainingSessions=" + sessions.size()
        );

        /*
         * Enforce maximum session capacity.
         */
        if (sessions.size() >= 10000) {

            log.warn(
                    "[AccountManager][login] Session creation rejected"
                            + " | reason=SESSION_CAPACITY_REACHED"
                            + " | activeSessions=" + sessions.size()
            );

            throw new IllegalArgumentException(
                    "Session capacity reached. Try again later."
            );
        }

        /*
         * Generate a cryptographically secure session token.
         *
         * Never log the token or its encoded representation.
         */
        log.debug(() ->
                "[AccountManager][login] Generating secure session token"
                        + " | randomBytes=32"
        );

        byte[] random = new byte[32];

        new SecureRandom().nextBytes(random);

        String token =
                Base64.getUrlEncoder()
                        .withoutPadding()
                        .encodeToString(random);

        AccountSession session = new AccountSession(
                token,
                username,
                Instant.now().plusSeconds(1800)
        );

        sessions.put(token, session);

        log.info(
                "[AccountManager][login] Login succeeded"
                        + " | username=" + username
        );

        log.debug(() ->
                "[AccountManager][login] Account session created successfully"
                        + " | activeSessions=" + sessions.size()
                        + " | sessionExpiresAt=" + session.expiresAt()
        );

        return session;
    }

    /**
     * Retrieves a valid, non-expired account session.
     */
    public Optional<AccountSession> session(String token) {

        log.debug(() ->
                "[AccountManager][session] Session lookup requested"
                        + " | tokenPresent=" + (token != null)
        );

        AccountSession session =
                token == null
                        ? null
                        : sessions.get(token);

        if (session == null) {

            log.debug(() ->
                    "[AccountManager][session] Session lookup unsuccessful"
                            + " | reason=SESSION_NOT_FOUND"
            );

            return Optional.empty();
        }

        /*
         * Remove expired sessions during lookup.
         */
        if (!Instant.now().isBefore(session.expiresAt())) {

            log.debug(() ->
                    "[AccountManager][session] Expired session detected"
                            + " | action=REMOVE_SESSION"
            );

            sessions.remove(token);

            log.debug(() ->
                    "[AccountManager][session] Expired session removed"
                            + " | remainingSessions=" + sessions.size()
            );

            return Optional.empty();
        }

        log.debug(() ->
                "[AccountManager][session] Valid account session found"
        );

        return Optional.of(session);
    }

    /**
     * Revokes an account session.
     */
    public void logout(String token) {

        log.debug(() ->
                "[AccountManager][logout] Account logout requested"
                        + " | tokenPresent=" + (token != null)
        );

        if (token != null) {

            AccountSession removed = sessions.remove(token);

            log.debug(() ->
                    "[AccountManager][logout] Session removal completed"
                            + " | sessionRemoved=" + (removed != null)
                            + " | remainingSessions=" + sessions.size()
            );

        } else {

            log.debug(() ->
                    "[AccountManager][logout] Logout skipped"
                            + " | reason=NULL_SESSION_TOKEN"
            );
        }
    }

    /**
     * Validates usernames before database operations.
     */
    private void validateUsername(String username) {

        log.debug(() ->
                "[AccountManager][validateUsername] Validating username"
        );

        if (username == null
                || !username.matches(
                "[a-zA-Z0-9][a-zA-Z0-9._-]{2,29}"
        )) {

            log.debug(() ->
                    "[AccountManager][validateUsername] Username validation failed"
            );

            throw new IllegalArgumentException(
                    "Username must contain 3–30 letters, digits, " +
                            "dots, underscores or hyphens."
            );
        }

        log.debug(() ->
                "[AccountManager][validateUsername] Username validation completed"
        );
    }

    /**
     * Generates a salted PBKDF2 password hash.
     */
    private String hash(char[] password) {

        log.debug(() ->
                "[AccountManager][hash] Starting password hash generation"
                        + " | algorithm=PBKDF2WithHmacSHA256"
                        + " | iterations=" + ITERATIONS
        );

        byte[] salt = new byte[16];

        new SecureRandom().nextBytes(salt);

        String encoded =
                "pbkdf2-sha256$"
                        + ITERATIONS
                        + "$"
                        + Base64.getEncoder().encodeToString(salt)
                        + "$"
                        + Base64.getEncoder().encodeToString(
                        derive(password, salt, ITERATIONS)
                );

        log.debug(() ->
                "[AccountManager][hash] Password hash generation completed"
        );

        return encoded;
    }

    /**
     * Returns the dummy password hash used when an
     * account does not exist.
     */
    private String dummyHash() {

        return "pbkdf2-sha256$600000$"
                + "AAAAAAAAAAAAAAAAAAAAAA==$"
                + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    }

    /**
     * Verifies a password against a stored PBKDF2 hash.
     */
    private boolean verify(
            char[] password,
            String encoded
    ) {

        log.debug(() ->
                "[AccountManager][verify] Starting password verification"
        );

        try {

            String[] parts = encoded.split("\\$");

            /*
             * Validate the stored hash structure.
             */
            if (parts.length != 4
                    || !parts[0].equals("pbkdf2-sha256")) {

                log.warn(
                        "[AccountManager][verify] Unsupported or malformed password hash"
                );

                return false;
            }

            int rounds = Integer.parseInt(parts[1]);

            /*
             * Reject hashes with unsupported iteration counts.
             */
            if (rounds < ITERATIONS || rounds > 2_000_000) {

                log.warn(
                        "[AccountManager][verify] Password hash iteration count rejected"
                                + " | rounds=" + rounds
                );

                return false;
            }

            /*
             * Derive and compare the password hash.
             */
            boolean matches = MessageDigest.isEqual(
                    Base64.getDecoder().decode(parts[3]),
                    derive(
                            password,
                            Base64.getDecoder().decode(parts[2]),
                            rounds
                    )
            );

            log.debug(() ->
                    "[AccountManager][verify] Password verification completed"
            );

            return matches;

        } catch (IllegalArgumentException exception) {

            log.warn(
                    "[AccountManager][verify] Password hash verification rejected"
                            + " | reason=INVALID_HASH_FORMAT"
            );

            return false;
        }
    }

    /**
     * Derives a 256-bit key using PBKDF2 with HMAC-SHA256.
     */
    private byte[] derive(
            char[] password,
            byte[] salt,
            int rounds
    ) {

        log.debug(() ->
                "[AccountManager][derive] Starting PBKDF2 key derivation"
                        + " | iterations=" + rounds
                        + " | keyLengthBits=256"
        );

        PBEKeySpec spec = new PBEKeySpec(
                password,
                salt,
                rounds,
                256
        );

        try {

            byte[] derivedKey =
                    SecretKeyFactory.getInstance(
                                    "PBKDF2WithHmacSHA256"
                            )
                            .generateSecret(spec)
                            .getEncoded();

            log.debug(() ->
                    "[AccountManager][derive] PBKDF2 key derivation completed"
            );

            return derivedKey;

        } catch (java.security.GeneralSecurityException exception) {

            log.error(
                    "[AccountManager][derive] Password key derivation failed"
                            + " | exception=" + exception.getClass().getSimpleName()
            );

            throw new IllegalStateException(
                    "Password hashing unavailable",
                    exception
            );

        } finally {

            spec.clearPassword();
        }
    }
}