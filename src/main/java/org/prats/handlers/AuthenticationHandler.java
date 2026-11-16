
package org.prats.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import org.prats.definitions.classes.Message;

import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.enums.Permission;

import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.definitions.interfaces.ReplyChannel;

import org.prats.definitions.records.DatabaseRequest;
import org.prats.definitions.records.DatabaseResponse;
import org.prats.definitions.records.Session;

import org.prats.loggers.EngineLogger;
import org.prats.managers.InMemorySessionManager;
import org.prats.parsers.MessageParser;
import org.prats.replyChannels.FutureReplyChannel;
import org.prats.security.hasher.PasswordHasher;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Processes authentication and account-management Messages.
 *
 * Responsibilities:
 *
 * 1. Authenticate user credentials.
 * 2. Retrieve account records through DatabaseService.
 * 3. Verify password hashes.
 * 4. Create authenticated sessions.
 * 5. Authorize account creation.
 * 6. Create accounts using password hashes.
 * 7. Revoke sessions during logout.
 * 8. Dispatch standard Message responses.
 *
 * AuthenticationHandler does not:
 *
 * - Manage TCP connections.
 * - Own the session registry.
 * - Perform JDBC operations directly.
 * - Own request/response correlation.
 * - Return authentication response futures.
 *
 * MessageService manages response correlation.
 *
 * FutureReplyChannel is used only for internal
 * asynchronous database requests.
 *
 * SECURITY:
 *
 * Passwords, password hashes and session credentials
 * must never appear in application logs.
 *
 * Account creation requires an authenticated caller
 * possessing the configured creation permission.
 *
 * A client-supplied process type, username or
 * permission list cannot bypass authorization.
 */
public final class AuthenticationHandler {

    private static final EngineLogger log =
            EngineLogger.getLogger(
                    AuthenticationHandler.class
            );

    // =========================================================
    // 1. CONFIGURATION
    // =========================================================

    private static final String USERS_ENTITY = "users";

    private static final String USERNAME_COLUMN = "user_name";

    private static final String PASSWORD_COLUMN = "password";

    private static final String ACTIVE_COLUMN = "active";

    private static final String PERMISSIONS_COLUMN = "permissions";

    private static final long SESSION_TTL_SECONDS = 1800;

    private static final long DATABASE_TIMEOUT_SECONDS = 15;

    private static final int MIN_PASSWORD_LENGTH = 10;

    private static final int MAX_PASSWORD_LENGTH = 128;

    private static final String USERNAME_PATTERN =
            "[a-zA-Z0-9][a-zA-Z0-9._-]{2,29}";

    /**
     * Recognized database value for an active account.
     */
    private static final String ACTIVE_VALUE = "Y";

    /**
     * Fallback hash used when an account is absent.
     *
     * Verification still performs PBKDF2 work,
     * reducing username-existence timing differences.
     *
     * The salt and expected hash are fixed zero bytes.
     * This is not a real account password hash.
     */
    private static final String DUMMY_PASSWORD_HASH =
            "PBKDF2WithHmacSHA256$600000$"
                    + "AAAAAAAAAAAAAAAAAAAAAA==$"
                    + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

    // =========================================================
    // 2. ERROR CODES
    // =========================================================

    private static final String ERROR_INVALID_OPERATION =
            "INVALID_AUTH_OPERATION";

    private static final String ERROR_INVALID_REQUEST =
            "INVALID_AUTH_REQUEST";

    private static final String ERROR_INVALID_CREDENTIALS =
            "INVALID_CREDENTIALS";

    private static final String ERROR_TOO_MANY_ATTEMPTS =
            "TOO_MANY_LOGIN_ATTEMPTS";

    private static final String ERROR_DATABASE_UNAVAILABLE =
            "AUTH_DATABASE_UNAVAILABLE";

    private static final String ERROR_INTERNAL =
            "AUTH_INTERNAL_ERROR";

    private static final String ERROR_MISSING_SESSION =
            "MISSING_SESSION_ID";

    private static final String ERROR_INVALID_SESSION =
            "INVALID_SESSION";

    private static final String ERROR_PERMISSION_DENIED =
            "ACCOUNT_PERMISSION_DENIED";

    private static final String ERROR_CREATION_DISABLED =
            "ACCOUNT_CREATION_DISABLED";

    private static final String ERROR_INVALID_ACCOUNT =
            "INVALID_ACCOUNT_DATA";

    private static final String ERROR_ACCOUNT_CREATION =
            "ACCOUNT_CREATION_FAILED";

    private static final String ERROR_SESSION_CREATION =
            "SESSION_CREATION_FAILED";

    // =========================================================
    // 3. DEPENDENCIES
    // =========================================================

    private final MessageDispatcher messageDispatcher;

    private final InMemorySessionManager inMemorySessionManager;

    private final PasswordHasher passwordHasher;

    private final MessageParser messageParser;

    /**
     * Executes expensive password hashing and
     * verification operations.
     *
     * A dedicated bounded executor is preferable
     * to the common pool in production.
     */
    private final Executor credentialExecutor;

    /**
     * Required permission for creating accounts.
     *
     * Null disables account creation.
     */
    private final Permission createUserPermission;

    // =========================================================
    // 4. SERVICE COMPLETION CALLBACKS
    // =========================================================

    /**
     * Notifies AuthenticationService when a terminal
     * response has been submitted to MessageService.
     *
     * This is lifecycle bookkeeping, not a second
     * response-correlation mechanism.
     */
    private volatile Consumer<Message> responseObserver;

    /**
     * Notifies AuthenticationService when a terminal
     * response cannot be submitted.
     */
    private volatile BiConsumer<UUID, Throwable> failureObserver;

    // =========================================================
    // 5. CONSTRUCTORS
    // =========================================================

    /**
     * Compatibility constructor.
     *
     * Account creation remains disabled because
     * no required permission has been configured.
     *
     * Uses the common pool for credential work.
     */
    public AuthenticationHandler(
            MessageDispatcher messageDispatcher,
            InMemorySessionManager inMemorySessionManager,
            PasswordHasher passwordHasher,
            MessageParser messageParser
    ) {

        this(
                messageDispatcher,
                inMemorySessionManager,
                passwordHasher,
                messageParser,
                null,
                ForkJoinPool.commonPool()
        );
    }

    /**
     * Constructor with account-creation permission.
     */
    public AuthenticationHandler(
            MessageDispatcher messageDispatcher,
            InMemorySessionManager inMemorySessionManager,
            PasswordHasher passwordHasher,
            MessageParser messageParser,
            Permission createUserPermission
    ) {

        this(
                messageDispatcher,
                inMemorySessionManager,
                passwordHasher,
                messageParser,
                createUserPermission,
                ForkJoinPool.commonPool()
        );
    }

    /**
     * Fully configurable constructor.
     *
     * The credential executor should be a bounded,
     * lifecycle-managed executor supplied by Platform.
     */
    public AuthenticationHandler(
            MessageDispatcher messageDispatcher,
            InMemorySessionManager inMemorySessionManager,
            PasswordHasher passwordHasher,
            MessageParser messageParser,
            Permission createUserPermission,
            Executor credentialExecutor
    ) {

        this.messageDispatcher =
                Objects.requireNonNull(
                        messageDispatcher,
                        "MessageDispatcher cannot be null"
                );

        this.inMemorySessionManager =
                Objects.requireNonNull(
                        inMemorySessionManager,
                        "SessionManager cannot be null"
                );

        this.passwordHasher =
                Objects.requireNonNull(
                        passwordHasher,
                        "PasswordHasher cannot be null"
                );

        this.messageParser =
                Objects.requireNonNull(
                        messageParser,
                        "MessageParser cannot be null"
                );

        this.credentialExecutor =
                Objects.requireNonNull(
                        credentialExecutor,
                        "Credential executor cannot be null"
                );

        this.createUserPermission = createUserPermission;

        log.info(() ->
                "[AuthenticationHandler][constructor] "
                        + "Authentication handler initialized"
                        + " | accountCreationEnabled="
                        + (createUserPermission != null)
                        + " | sessionTTLSeconds="
                        + SESSION_TTL_SECONDS
                        + " | databaseTimeoutSeconds="
                        + DATABASE_TIMEOUT_SECONDS
        );
    }

    // =========================================================
    // 6. CONFIGURE SERVICE COMPLETION NOTIFICATIONS
    // =========================================================

    /**
     * Configures lifecycle notifications.
     *
     * AuthenticationService uses these notifications
     * to release admitted request permits.
     *
     * Configuration must occur before the service
     * begins accepting requests.
     */
    public void setCompletionCallbacks(
            Consumer<Message> responseObserver,
            BiConsumer<UUID, Throwable> failureObserver
    ) {

        this.responseObserver =
                Objects.requireNonNull(
                        responseObserver,
                        "Response observer cannot be null"
                );

        this.failureObserver =
                Objects.requireNonNull(
                        failureObserver,
                        "Failure observer cannot be null"
                );

        log.debug(
                "[AuthenticationHandler][setCompletionCallbacks] "
                        + "Service lifecycle callbacks configured"
        );
    }

    // =========================================================
    // 7. PROCESS AUTHENTICATION REQUEST
    // =========================================================

    /**
     * Routes authentication operations.
     *
     * LOGIN and AUTHENTICATE both initiate credential
     * authentication in this implementation.
     *
     * CREATE_USER requires an existing privileged session.
     *
     * LOGOUT requires an existing valid session.
     */
    public void processRequest(
            Message authenticationRequest
    ) {

        Objects.requireNonNull(
                authenticationRequest,
                "Authentication request cannot be null"
        );

        log.debug(() ->
                "[AuthenticationHandler][processRequest] "
                        + "Authentication request received"
                        + " | operation="
                        + authenticationRequest.getOperation()
                        + " | correlationId="
                        + authenticationRequest.getCorrelationId()
                        + " | sessionPresent="
                        + (authenticationRequest.getSessionId() != null)
        );

        try {

            Operation operation =
                    authenticationRequest.getOperation();

            if (operation == null) {

                sendInvalidResponse(
                        authenticationRequest,
                        ERROR_INVALID_OPERATION,
                        "Authentication operation is missing"
                );

                return;
            }

            switch (operation) {

                case LOGIN, AUTHENTICATE ->
                        handleLoginRequest(authenticationRequest);

                case CREATE_USER ->
                        handleCreateUserRequest(authenticationRequest);

                case LOGOUT ->
                        handleLogoutRequest(authenticationRequest);

                default ->
                        sendInvalidResponse(
                                authenticationRequest,
                                ERROR_INVALID_OPERATION,
                                "Unsupported authentication operation"
                        );
            }

        } catch (RuntimeException exception) {

            log.error(
                    "[AuthenticationHandler][processRequest] "
                            + "Unexpected authentication processing failure",
                    exception
            );

            sendInvalidResponse(
                    authenticationRequest,
                    ERROR_INTERNAL,
                    "Unable to process authentication request"
            );
        }
    }

    // =========================================================
    // 8. CREATE DATABASE REQUEST
    // =========================================================

    /**
     * Creates a typed database request.
     *
     * DatabaseRequest is serialized into the payload
     * of a standard Mediation Engine Message.
     *
     * The requested database operation remains
     * in Message.operation.
     */
    private DatabaseRequest createDatabaseRequest(
            String entity,
            List<String> columns,
            Map<String, Object> data,
            DatabaseRequest.Filter filters
    ) {

        log.debug(() ->
                "[AuthenticationHandler][createDatabaseRequest] "
                        + "Creating typed database request"
                        + " | entity=" + entity
                        + " | columnCount=" + columns.size()
                        + " | hasFilters=" + (filters != null)
        );

        return new DatabaseRequest(
                entity,
                columns,
                data,
                filters
        );
    }

    // =========================================================
    // 9. DISPATCH DATABASE REQUEST
    // =========================================================

    /**
     * Dispatches an internal database request.
     *
     * Each database request receives a new correlation
     * ID, independent of the original authentication
     * request.
     *
     * FutureReplyChannel receives the database response.
     *
     * MessageService remains responsible for matching
     * that response with the registered ReplyChannel.
     *
     * Account database capability is set locally.
     */
    private CompletableFuture<DatabaseResponse> dispatchDatabaseRequest(
            Operation operation,
            DatabaseRequest databaseRequest
    ) {

        Message databaseMessage =
                messageParser.createEmptyMessage();

        databaseMessage.setMessageType(
                MessageType.REQUEST
        );

        databaseMessage.setOperation(operation);

        databaseMessage.setPayload(
                messageParser.toPayload(databaseRequest)
        );

        /*
         * This local marker must be assigned only
         * by trusted server-side authentication code.
         *
         * It is excluded from JSON serialization.
         */
        databaseMessage.setAccountDatabaseRequest(true);

        CompletableFuture<Message> responseFuture =
                new CompletableFuture<>();

        ReplyChannel replyChannel =
                new FutureReplyChannel(responseFuture);

        log.debug(() ->
                "[AuthenticationHandler][dispatchDatabaseRequest] "
                        + "Dispatching internal database request"
                        + " | operation=" + operation
                        + " | databaseCorrelationId="
                        + databaseMessage.getCorrelationId()
        );

        try {

            messageDispatcher.dispatchRequest(
                    databaseMessage,
                    replyChannel
            );

        } catch (RuntimeException exception) {

            log.error(
                    "[AuthenticationHandler][dispatchDatabaseRequest] "
                            + "Database request submission failed",
                    exception
            );

            responseFuture.completeExceptionally(exception);
        }

        /*
         * A timeout prevents the authentication
         * workflow from waiting indefinitely.
         *
         * MessageService remains responsible for
         * its own correlation expiry and cleanup.
         */
        return responseFuture
                .orTimeout(
                        DATABASE_TIMEOUT_SECONDS,
                        TimeUnit.SECONDS
                )
                .thenApply(response -> {

                    if (response == null || response.hasError()) {

                        throw new IllegalStateException(
                                "Database request failed"
                        );
                    }

                    try {

                        DatabaseResponse databaseResponse =
                                messageParser.fromPayload(
                                        response.getPayload(),
                                        DatabaseResponse.class
                                );

                        log.debug(() ->
                                "[AuthenticationHandler][dispatchDatabaseRequest] "
                                        + "Database response decoded"
                                        + " | operation=" + operation
                                        + " | rowCount="
                                        + databaseResponse.rows().size()
                        );

                        return databaseResponse;

                    } catch (JsonProcessingException exception) {

                        throw new CompletionException(exception);
                    }
                });
    }

    // =========================================================
    // 10. GET STORED USER RECORD
    // =========================================================

    /**
     * Retrieves account information using a typed
     * SEARCH request.
     *
     * Lookup fields:
     *
     * - user_name
     * - password
     * - active
     * - permissions
     *
     * The password field contains a stored hash,
     * never a plaintext password.
     */
    private CompletableFuture<DatabaseResponse> getStoredHash(
            String username
    ) {

        DatabaseRequest.Filter filter =
                new DatabaseRequest.Condition(
                        USERNAME_COLUMN,
                        DatabaseRequest.ComparisonOperator.EQ,
                        username
                );

        DatabaseRequest databaseRequest =
                createDatabaseRequest(
                        USERS_ENTITY,
                        List.of(
                                USERNAME_COLUMN,
                                PASSWORD_COLUMN,
                                ACTIVE_COLUMN,
                                PERMISSIONS_COLUMN
                        ),
                        Map.of(),
                        filter
                );

        return dispatchDatabaseRequest(
                Operation.SEARCH,
                databaseRequest
        );
    }

    // =========================================================
    // 11. HANDLE LOGIN REQUEST
    // =========================================================

    /**
     * Authenticates the supplied username and password.
     *
     * Sequence:
     *
     * 1. Validate login payload.
     * 2. Check recent login failures.
     * 3. Retrieve stored credentials.
     * 4. Verify password on the credential executor.
     * 5. Verify account status.
     * 6. Resolve server-stored permissions.
     * 7. Register an authenticated session.
     * 8. Dispatch a standard success response.
     *
     * Password verification never runs directly
     * on MessageService's database-response thread.
     */
    private void handleLoginRequest(
            Message login
    ) {

        JsonNode payload = login.getPayload();

        if (payload == null || !payload.isObject()) {

            sendInvalidResponse(
                    login,
                    ERROR_INVALID_REQUEST,
                    "Invalid login request"
            );

            return;
        }

        String username =
                readString(payload, "username");

        String passwordText =
                readString(payload, "password");

        if (!isValidUsername(username)
                || passwordText == null
                || passwordText.isEmpty()
                || passwordText.length() > MAX_PASSWORD_LENGTH) {

            sendInvalidResponse(
                    login,
                    ERROR_INVALID_REQUEST,
                    "Invalid username or password"
            );

            return;
        }

        String normalizedUsername =
                normalizeUsername(username);

        /*
         * Password characters are copied into a
         * temporary array for cryptographic processing.
         *
         * The original JSON string cannot be explicitly
         * wiped from memory.
         */
        char[] password =
                passwordText.toCharArray();

        /*
         * Release the local plaintext String reference.
         * The JsonNode may still retain its value.
         */
        passwordText = null;

        log.debug(() ->
                "[AuthenticationHandler][handleLoginRequest] "
                        + "Login request validated"
                        + " | userId=" + normalizedUsername
                        + " | correlationId="
                        + login.getCorrelationId()
        );

        // -----------------------------------------------------
        // REQUEST DATABASE LOOKUP
        // -----------------------------------------------------

        CompletableFuture<DatabaseResponse> lookup;

        try {

            lookup = getStoredHash(normalizedUsername);

        } catch (RuntimeException exception) {

            Arrays.fill(password, '\0');

            log.error(
                    "[AuthenticationHandler][handleLoginRequest] "
                            + "Unable to initiate credential lookup",
                    exception
            );

            sendInvalidResponse(
                    login,
                    ERROR_DATABASE_UNAVAILABLE,
                    "Authentication storage is unavailable"
            );

            return;
        }

        // -----------------------------------------------------
        // PROCESS DATABASE RESPONSE ASYNCHRONOUSLY
        // -----------------------------------------------------

        CompletableFuture<Void> continuation =
                lookup.thenAcceptAsync(
                        databaseResponse -> {

                            try {

                                completeLogin(
                                        login,
                                        normalizedUsername,
                                        password,
                                        databaseResponse
                                );

                            } finally {

                                Arrays.fill(password, '\0');
                            }
                        },
                        credentialExecutor
                );

        /*
         * Also handles:
         *
         * - Database lookup timeout.
         * - Database decoding failure.
         * - Rejected credential executor submission.
         * - Unexpected callback exceptions.
         */
        continuation.exceptionally(exception -> {

            Arrays.fill(password, '\0');

            Throwable cause = unwrap(exception);

            log.error(
                    "[AuthenticationHandler][handleLoginRequest] "
                            + "Asynchronous login workflow failed",
                    cause
            );

            sendInvalidResponse(
                    login,
                    ERROR_DATABASE_UNAVAILABLE,
                    "Unable to complete authentication"
            );

            return null;
        });
    }

    // =========================================================
    // 12. COMPLETE LOGIN
    // =========================================================

    /**
     * Verifies credentials and creates the session.
     *
     * A missing account still performs password
     * verification against a dummy stored hash.
     *
     * Invalid credentials and disabled accounts
     * receive the same generic response.
     */
    private void completeLogin(
            Message originalRequest,
            String username,
            char[] password,
            DatabaseResponse databaseResponse
    ) {

        List<Map<String, Object>> rows =
                databaseResponse.rows();

        Map<String, Object> userRecord =
                rows == null || rows.isEmpty()
                        ? Map.of()
                        : rows.getFirst();

        String storedHash =
                readColumn(userRecord, PASSWORD_COLUMN);

        String active =
                readColumn(userRecord, ACTIVE_COLUMN);

        String permissionsValue =
                readColumn(userRecord, PERMISSIONS_COLUMN);

        // -----------------------------------------------------
        // VERIFY PASSWORD
        // -----------------------------------------------------

        boolean validPassword =
                passwordHasher.verifyPassword(
                        password,
                        storedHash == null
                                ? DUMMY_PASSWORD_HASH
                                : storedHash
                );

        boolean activeAccount =
                ACTIVE_VALUE.equalsIgnoreCase(active);

        if (!validPassword || !activeAccount) {

            log.warn(() ->
                    "[AuthenticationHandler][completeLogin] "
                            + "Login rejected"
                            + " | userId=" + username
                            + " | reason=INVALID_CREDENTIALS_OR_ACCOUNT_STATUS"
            );

            sendInvalidResponse(
                    originalRequest,
                    ERROR_INVALID_CREDENTIALS,
                    "Invalid username or password"
            );

            return;
        }

        // -----------------------------------------------------
        // LOAD AUTHORITATIVE PERMISSIONS
        // -----------------------------------------------------

        Set<Permission> permissions;

        try {

            permissions = parsePermissions(
                    permissionsValue
            );

        } catch (IllegalArgumentException exception) {

            log.error(
                    "[AuthenticationHandler][completeLogin] "
                            + "Stored account permissions are invalid",
                    exception
            );

            sendInvalidResponse(
                    originalRequest,
                    ERROR_INTERNAL,
                    "Unable to load account permissions"
            );

            return;
        }

        // -----------------------------------------------------
        // CREATE SESSION
        // -----------------------------------------------------

        Session session;

        try {

            session =
                    inMemorySessionManager.registerSession(
                            username,
                            permissions,
                            SESSION_TTL_SECONDS
                    );

        } catch (RuntimeException exception) {

            log.warn(() ->
                    "[AuthenticationHandler][completeLogin] "
                            + "Session registration rejected"
                            + " | userId=" + username
            );

            sendInvalidResponse(
                    originalRequest,
                    ERROR_SESSION_CREATION,
                    "Unable to establish an authenticated session"
            );

            return;
        }

        // -----------------------------------------------------
        // CREATE SUCCESS RESPONSE
        // -----------------------------------------------------

        Message response =
                messageParser.createResponse(
                        originalRequest.getCorrelationId(),
                        Map.of(
                                "username",
                                session.username(),
                                "permissions",
                                session.permissions(),
                                "expiresAt",
                                session.expiresAt()
                        )
                );

        /*
         * The session credential is placed in the
         * Message envelope, not the business payload.
         */
        response.setSessionId(
                session.sessionId()
        );

        boolean dispatched =
                sendSuccessResponse(
                        originalRequest,
                        response
                );

        if (!dispatched) {

            /*
             * The login response was not accepted
             * for dispatch.
             *
             * Revoke the newly created session
             * because its credential was not delivered
             * to the normal response pipeline.
             */
            inMemorySessionManager.logout(
                    session.sessionId()
            );

            return;
        }

        log.info(() ->
                "[AuthenticationHandler][completeLogin] "
                        + "Authentication completed successfully"
                        + " | userId=" + session.username()
                        + " | expiresAt=" + session.expiresAt()
                        + " | permissionCount="
                        + session.permissions().size()
        );
    }

    // =========================================================
    // 13. CREATE USER
    // =========================================================

    /**
     * Creates a user account after authorization.
     *
     * The caller must have:
     *
     * 1. A valid existing session.
     * 2. The configured account-creation permission.
     *
     * Permissions assigned to the new account must
     * be contained in the creator's permissions.
     *
     * No public unauthenticated registration is allowed.
     */
    private void handleCreateUserRequest(
            Message createAccountRequest
    ) {

        // -----------------------------------------------------
        // VERIFY ACCOUNT CREATION CONFIGURATION
        // -----------------------------------------------------

        if (createUserPermission == null) {

            log.warn(
                    "[AuthenticationHandler][handleCreateUserRequest] "
                            + "Account creation is disabled"
            );

            sendInvalidResponse(
                    createAccountRequest,
                    ERROR_CREATION_DISABLED,
                    "Account creation is not configured"
            );

            return;
        }

        // -----------------------------------------------------
        // VALIDATE CALLER SESSION
        // -----------------------------------------------------

        UUID sessionId =
                createAccountRequest.getSessionId();

        Optional<Session> caller =
                inMemorySessionManager.getSession(sessionId);

        if (caller.isEmpty()) {

            sendInvalidResponse(
                    createAccountRequest,
                    ERROR_INVALID_SESSION,
                    "Authentication is required"
            );

            return;
        }

        Session authenticatedCaller =
                caller.get();

        // -----------------------------------------------------
        // CHECK ACCOUNT CREATION PERMISSION
        // -----------------------------------------------------

        if (!authenticatedCaller.permissions()
                .contains(createUserPermission)) {

            log.warn(() ->
                    "[AuthenticationHandler][handleCreateUserRequest] "
                            + "Unauthorized account creation attempt"
                            + " | caller=" + authenticatedCaller.username()
                            + " | requiredPermission="
                            + createUserPermission
            );

            sendInvalidResponse(
                    createAccountRequest,
                    ERROR_PERMISSION_DENIED,
                    "Insufficient permissions to create accounts"
            );

            return;
        }

        // -----------------------------------------------------
        // VALIDATE ACCOUNT PAYLOAD
        // -----------------------------------------------------

        JsonNode payload =
                createAccountRequest.getPayload();

        if (payload == null || !payload.isObject()) {

            sendInvalidResponse(
                    createAccountRequest,
                    ERROR_INVALID_ACCOUNT,
                    "Invalid account creation request"
            );

            return;
        }

        String requestedUsername =
                readString(payload, "username");

        String passwordText =
                readString(payload, "password");

        if (!isValidUsername(requestedUsername)
                || passwordText == null
                || passwordText.length() < MIN_PASSWORD_LENGTH
                || passwordText.length() > MAX_PASSWORD_LENGTH) {

            sendInvalidResponse(
                    createAccountRequest,
                    ERROR_INVALID_ACCOUNT,
                    "Invalid account username or password"
            );

            return;
        }

        String username =
                normalizeUsername(requestedUsername);

        Set<Permission> requestedPermissions;

        try {

            requestedPermissions =
                    parseRequestedPermissions(
                            payload.path("permissions")
                    );

        } catch (IllegalArgumentException exception) {

            sendInvalidResponse(
                    createAccountRequest,
                    ERROR_INVALID_ACCOUNT,
                    "Invalid requested account permissions"
            );

            return;
        }

        // -----------------------------------------------------
        // PREVENT PERMISSION ESCALATION
        // -----------------------------------------------------

        if (!authenticatedCaller.permissions()
                .containsAll(requestedPermissions)) {

            log.warn(() ->
                    "[AuthenticationHandler][handleCreateUserRequest] "
                            + "Account creation rejected"
                            + " | reason=PERMISSION_ESCALATION"
                            + " | caller=" + authenticatedCaller.username()
            );

            sendInvalidResponse(
                    createAccountRequest,
                    ERROR_PERMISSION_DENIED,
                    "Cannot assign permissions not held by the caller"
            );

            return;
        }

        char[] password =
                passwordText.toCharArray();

        passwordText = null;

        log.debug(() ->
                "[AuthenticationHandler][handleCreateUserRequest] "
                        + "Account creation authorized"
                        + " | requestedUsername=" + username
                        + " | caller=" + authenticatedCaller.username()
                        + " | requestedPermissionCount="
                        + requestedPermissions.size()
        );

        // -----------------------------------------------------
        // HASH PASSWORD AND CREATE ACCOUNT
        // -----------------------------------------------------

        CompletableFuture<Void> continuation =
                CompletableFuture.runAsync(
                        () -> {

                            try {

                                createUserInDatabase(
                                        createAccountRequest,
                                        username,
                                        password,
                                        requestedPermissions
                                );

                            } finally {

                                Arrays.fill(password, '\0');
                            }
                        },
                        credentialExecutor
                );

        continuation.exceptionally(exception -> {

            Arrays.fill(password, '\0');

            log.error(
                    "[AuthenticationHandler][handleCreateUserRequest] "
                            + "Asynchronous account creation failed",
                    unwrap(exception)
            );

            sendInvalidResponse(
                    createAccountRequest,
                    ERROR_ACCOUNT_CREATION,
                    "Unable to create account"
            );

            return null;
        });
    }

    // =========================================================
    // 14. CREATE ACCOUNT DATABASE RECORD
    // =========================================================

    /**
     * Hashes the password and submits a typed
     * CREATE request to DatabaseService.
     *
     * Plaintext passwords are never included
     * in the database request.
     */
    private void createUserInDatabase(
            Message originalRequest,
            String username,
            char[] password,
            Set<Permission> permissions
    ) {

        String storedHash =
                passwordHasher.hashPassword(password);

        String permissionString =
                permissions.stream()
                        .map(Enum::name)
                        .sorted()
                        .collect(Collectors.joining(","));

        Map<String, Object> accountData =
                Map.of(
                        USERNAME_COLUMN, username,
                        PASSWORD_COLUMN, storedHash,
                        ACTIVE_COLUMN, ACTIVE_VALUE,
                        PERMISSIONS_COLUMN, permissionString
                );

        DatabaseRequest databaseRequest =
                createDatabaseRequest(
                        USERS_ENTITY,
                        List.of(),
                        accountData,
                        null
                );

        log.debug(() ->
                "[AuthenticationHandler][createUserInDatabase] "
                        + "Submitting account creation request"
                        + " | username=" + username
                        + " | permissionCount=" + permissions.size()
        );

        dispatchDatabaseRequest(
                Operation.CREATE,
                databaseRequest
        ).whenComplete((databaseResponse, exception) -> {

            if (exception != null) {

                log.warn(() ->
                        "[AuthenticationHandler][createUserInDatabase] "
                                + "Account creation database request failed"
                                + " | username=" + username
                                + " | failureType="
                                + unwrap(exception)
                                .getClass()
                                .getSimpleName()
                );

                sendInvalidResponse(
                        originalRequest,
                        ERROR_ACCOUNT_CREATION,
                        "Unable to create account"
                );

                return;
            }

            Message response =
                    messageParser.createResponse(
                            originalRequest.getCorrelationId(),
                            Map.of(
                                    "username", username,
                                    "created", true
                            )
                    );

            sendSuccessResponse(
                    originalRequest,
                    response
            );

            log.info(() ->
                    "[AuthenticationHandler][createUserInDatabase] "
                            + "Account creation completed"
                            + " | username=" + username
            );
        });
    }

    // =========================================================
    // 15. AUTHENTICATE OPERATION
    // =========================================================

    /**
     * Alias for the credential-based login operation.
     *
     * LOGIN and AUTHENTICATE currently use the same
     * username/password workflow.
     */
    private void handleAuthenticationRequest(
            Message authenticationRequest
    ) {

        handleLoginRequest(authenticationRequest);
    }

    // =========================================================
    // 16. LOGOUT
    // =========================================================

    /**
     * Revokes an existing authenticated session.
     *
     * Logout is independent of TCP connection state.
     *
     * The presented session must be valid at the
     * time of revocation.
     */
    private void handleLogoutRequest(
            Message logoutRequest
    ) {

        UUID sessionId =
                logoutRequest.getSessionId();

        if (sessionId == null) {

            sendInvalidResponse(
                    logoutRequest,
                    ERROR_MISSING_SESSION,
                    "Session identifier is required"
            );

            return;
        }

        Optional<Session> session =
                inMemorySessionManager.getSession(sessionId);

        if (session.isEmpty()) {

            sendInvalidResponse(
                    logoutRequest,
                    ERROR_INVALID_SESSION,
                    "Session is invalid or expired"
            );

            return;
        }

        String username =
                session.get().username();

        boolean revoked =
                inMemorySessionManager.logout(sessionId);

        if (!revoked) {

            sendInvalidResponse(
                    logoutRequest,
                    ERROR_INVALID_SESSION,
                    "Session could not be revoked"
            );

            return;
        }

        Message response =
                messageParser.createResponse(
                        logoutRequest.getCorrelationId(),
                        Map.of(
                                "loggedOut", true
                        )
                );

        /*
         * The logout response must not repeat
         * the revoked session credential.
         */
        response.setSessionId(null);

        sendSuccessResponse(
                logoutRequest,
                response
        );

        log.info(() ->
                "[AuthenticationHandler][handleLogoutRequest] "
                        + "Session logout completed"
                        + " | username=" + username
        );
    }

    // =========================================================
    // 17. SEND SUCCESS RESPONSE
    // =========================================================

    /**
     * Finalizes and dispatches a success response.
     *
     * The original correlation ID is preserved.
     *
     * No new response correlation registry is created.
     */
    private boolean sendSuccessResponse(
            Message originalRequest,
            Message response
    ) {

        response.setCorrelationId(
                originalRequest.getCorrelationId()
        );

        response.setMessageType(
                MessageType.RESPONSE
        );

        response.setStatus("SUCCESS");

        response.setResponseComplete(true);

        return dispatchTerminalResponse(
                originalRequest,
                response
        );
    }

    // =========================================================
    // 18. SEND ERROR RESPONSE
    // =========================================================

    /**
     * Creates and dispatches a standardized
     * authentication error response.
     *
     * Error codes are defined as class-level
     * constants to avoid inconsistencies.
     */
    private void sendInvalidResponse(
            Message originalRequest,
            String errorCode,
            String errorReason
    ) {

        log.debug(() ->
                "[AuthenticationHandler][sendInvalidResponse] "
                        + "Preparing authentication error response"
                        + " | errorCode=" + errorCode
                        + " | correlationId="
                        + originalRequest.getCorrelationId()
        );

        Message response =
                messageParser.createErrorResponse(
                        originalRequest.getCorrelationId(),
                        errorCode,
                        errorReason
                );

        /*
         * Error responses must not disclose
         * the incoming session credential.
         */
        response.setSessionId(null);

        dispatchTerminalResponse(
                originalRequest,
                response
        );
    }

    // =========================================================
    // 19. DISPATCH TERMINAL RESPONSE
    // =========================================================

    /**
     * Submits a terminal authentication response
     * to MessageService.
     *
     * After successful submission, AuthenticationService
     * is notified so admission resources can be released.
     *
     * This does not confirm delivery to the client.
     *
     * @return true if MessageService accepted the response.
     */
    private boolean dispatchTerminalResponse(
            Message originalRequest,
            Message response
    ) {

        try {

            messageDispatcher.dispatchResponse(response);

            log.debug(() ->
                    "[AuthenticationHandler][dispatchTerminalResponse] "
                            + "Authentication response submitted"
                            + " | correlationId="
                            + response.getCorrelationId()
                            + " | status=" + response.getStatus()
                            + " | hasError=" + response.hasError()
            );

        } catch (RuntimeException exception) {

            log.error(
                    "[AuthenticationHandler][dispatchTerminalResponse] "
                            + "Authentication response dispatch failed",
                    exception
            );

            notifyTerminalFailure(
                    originalRequest.getCorrelationId(),
                    exception
            );

            return false;
        }

        // -----------------------------------------------------
        // NOTIFY SERVICE ABOUT COMPLETION
        // -----------------------------------------------------

        Consumer<Message> observer =
                responseObserver;

        if (observer != null) {

            try {

                observer.accept(response);

            } catch (RuntimeException exception) {

                log.error(
                        "[AuthenticationHandler][dispatchTerminalResponse] "
                                + "Response completion observer failed",
                        exception
                );
            }

        } else {

            log.warn(
                    "[AuthenticationHandler][dispatchTerminalResponse] "
                            + "Response dispatched without a configured completion observer"
            );
        }

        return true;
    }

    // =========================================================
    // 20. NOTIFY TERMINAL FAILURE
    // =========================================================

    private void notifyTerminalFailure(
            UUID correlationId,
            Throwable exception
    ) {

        BiConsumer<UUID, Throwable> observer =
                failureObserver;

        if (observer == null) {

            log.warn(
                    "[AuthenticationHandler][notifyTerminalFailure] "
                            + "Terminal failure observer is not configured"
            );

            return;
        }

        try {

            observer.accept(
                    correlationId,
                    exception
            );

        } catch (RuntimeException observerException) {

            log.error(
                    "[AuthenticationHandler][notifyTerminalFailure] "
                            + "Terminal failure observer failed",
                    observerException
            );
        }
    }

    // =========================================================
    // 21. PERMISSION PARSING
    // =========================================================

    /**
     * Decodes permissions stored as comma-separated
     * enum names in the account database.
     *
     * Missing permissions grant no privileges.
     *
     * Unknown permission names cause validation failure.
     */
    private Set<Permission> parsePermissions(
            String encodedPermissions
    ) {

        if (encodedPermissions == null
                || encodedPermissions.isBlank()) {

            return Set.of();
        }

        return Arrays.stream(
                        encodedPermissions.split(",")
                )
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .map(value ->
                        Permission.valueOf(
                                value.toUpperCase(Locale.ROOT)
                        )
                )
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Reads an optional permission array from
     * an account-creation payload.
     *
     * An absent array produces an account with
     * no assigned permissions.
     */
    private Set<Permission> parseRequestedPermissions(
            JsonNode permissionsNode
    ) {

        if (permissionsNode == null
                || permissionsNode.isMissingNode()
                || permissionsNode.isNull()) {

            return Set.of();
        }

        if (!permissionsNode.isArray()) {

            throw new IllegalArgumentException(
                    "Permissions must be a JSON array"
            );
        }

        java.util.Set<Permission> permissions =
                new java.util.HashSet<>();

        for (JsonNode permissionNode : permissionsNode) {

            if (!permissionNode.isTextual()) {

                throw new IllegalArgumentException(
                        "Invalid permission value"
                );
            }

            Permission permission =
                    Permission.valueOf(
                            permissionNode.asText()
                                    .trim()
                                    .toUpperCase(Locale.ROOT)
                    );

            permissions.add(permission);
        }

        return Set.copyOf(permissions);
    }

    // =========================================================
    // 22. DATABASE FIELD EXTRACTION
    // =========================================================

    /**
     * Retrieves a field from a database row.
     *
     * Handles database drivers that return
     * uppercase or lowercase column names.
     */
    private String readColumn(
            Map<String, Object> row,
            String column
    ) {

        if (row == null || row.isEmpty()) {
            return null;
        }

        Object value =
                row.get(column);

        if (value == null) {

            value = row.get(
                    column.toUpperCase(Locale.ROOT)
            );
        }

        if (value == null) {

            value = row.get(
                    column.toLowerCase(Locale.ROOT)
            );
        }

        return value == null
                ? null
                : value.toString();
    }

    // =========================================================
    // 23. JSON FIELD EXTRACTION
    // =========================================================

    /**
     * Reads a textual field from a JSON object.
     *
     * Null, missing and non-textual fields
     * are rejected.
     */
    private String readString(
            JsonNode payload,
            String fieldName
    ) {

        JsonNode field =
                payload.get(fieldName);

        if (field == null
                || field.isNull()
                || !field.isTextual()) {

            return null;
        }

        return field.asText();
    }

    // =========================================================
    // 24. USERNAME VALIDATION
    // =========================================================

    private boolean isValidUsername(
            String username
    ) {

        return username != null
                && username.matches(USERNAME_PATTERN);
    }

    private String normalizeUsername(
            String username
    ) {

        return username.trim()
                .toUpperCase(Locale.ROOT);
    }

    // =========================================================
    // 25. EXCEPTION UNWRAPPING
    // =========================================================

    /**
     * Unwraps asynchronous completion exceptions
     * for more useful diagnostic logging.
     */
    private Throwable unwrap(
            Throwable exception
    ) {

        Throwable current = exception;

        while (current instanceof CompletionException
                && current.getCause() != null) {

            current = current.getCause();
        }

        return current;
    }
}
