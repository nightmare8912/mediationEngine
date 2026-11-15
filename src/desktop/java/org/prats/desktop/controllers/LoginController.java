
package org.prats.desktop.controllers;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.records.LoginRequest;
import org.prats.definitions.records.LoginResponse;
import org.prats.definitions.records.Session;
import org.prats.desktop.DesktopConnectionManager;
import org.prats.desktop.pages.LoginPage;
import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;
import org.prats.replyChannels.FutureReplyChannel;

import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class LoginController {

    private static final EngineLogger log =
            EngineLogger.getLogger(LoginController.class);

    // =========================================================
    // Constants
    // =========================================================

    private static final String PLATFORM_PROCESS = "PLATFORM";

    private static final long AUTHENTICATION_TIMEOUT_SECONDS = 20;

    /*
     * Authentication operations must not execute on
     * Swing's Event Dispatch Thread.
     *
     * Virtual threads are suitable for performing
     * the background authentication work.
     */
    private static final Executor AUTHENTICATION_EXECUTOR =
            task -> Thread.ofVirtual()
                    .name("desktop-authentication")
                    .start(task);

    // =========================================================
    // Dependencies
    // =========================================================

    private final LoginPage loginPage;

    private final DesktopConnectionManager desktopConnectionManager;

    private final MessageParser messageParser;

    /*
     * Invoked after authentication succeeds.
     *
     * The desktop application can provide a callback
     * to open the dashboard or initialize other pages.
     */
    private final Consumer<Session> onAuthenticated;

    /*
     * Prevent multiple simultaneous connection attempts
     * if the user clicks the login button repeatedly.
     */
    private final AtomicBoolean isConnecting =
            new AtomicBoolean(false);

    /*
     * Stores the authenticated session for subsequent
     * GUI operations.
     *
     * The Platform remains responsible for validating
     * the session on every protected request.
     */
    private volatile Session currentSession;

    // =========================================================
    // Constructors
    // =========================================================

    public LoginController(
            LoginPage loginPage,
            DesktopConnectionManager desktopConnectionManager,
            MessageParser messageParser
    ) {

        this(
                loginPage,
                desktopConnectionManager,
                messageParser,
                session -> {}
        );
    }

    public LoginController(
            LoginPage loginPage,
            DesktopConnectionManager desktopConnectionManager,
            MessageParser messageParser,
            Consumer<Session> onAuthenticated
    ) {

        this.loginPage = Objects.requireNonNull(
                loginPage,
                "Login page cannot be null"
        );

        this.desktopConnectionManager = Objects.requireNonNull(
                desktopConnectionManager,
                "Desktop connection manager cannot be null"
        );

        this.messageParser = Objects.requireNonNull(
                messageParser,
                "Message parser cannot be null"
        );

        this.onAuthenticated = Objects.requireNonNull(
                onAuthenticated,
                "Authentication callback cannot be null"
        );

        loginPage.addLoginListener(
                event -> handleLogin()
        );

        log.info(
                "[LoginController][constructor] Login controller initialized"
        );
    }

    // =========================================================
    // Platform Response Validation
    // =========================================================

    /*
     * Validates the final authentication response
     * received from the Platform.
     *
     * The response payload must conform to the
     * LoginResponse record contract.
     *
     * This function executes as part of the
     * authentication future's completion chain,
     * not as a Swing UI operation.
     */
    private Session validatePlatformResponse(
            Message response
    ) {

        log.debug(
                "[LoginController][validatePlatformResponse] "
                        + "Validating Platform authentication response"
        );

        if (response == null) {

            log.error(
                    "[LoginController][validatePlatformResponse] "
                            + "Platform returned a null response"
            );

            throw new IllegalStateException(
                    "Platform returned an empty authentication response"
            );
        }

        if (response.hasError()) {

            log.warn(
                    "[LoginController][validatePlatformResponse] "
                            + "Platform rejected authentication request"
            );

            throw new SecurityException(
                    "Authentication was rejected by the Platform"
            );
        }

        /*
         * Deserialize the JSON payload into the
         * expected LoginResponse record.
         *
         * We no longer need to manually extract
         * sessionId, permissions, or timestamps
         * from individual JsonNode fields.
         */
        LoginResponse loginResponse;

        try {

            loginResponse = messageParser.fromPayload(
                    response.getPayload(),
                    LoginResponse.class
            );

        } catch (Exception exception) {

            log.error(
                    "[LoginController][validatePlatformResponse] "
                            + "Unable to deserialize LoginResponse payload",
                    exception
            );

            throw new IllegalStateException(
                    "Invalid authentication response received from Platform",
                    exception
            );
        }

        if (loginResponse == null ||
                loginResponse.session() == null) {

            log.error(
                    "[LoginController][validatePlatformResponse] "
                            + "Authentication response does not contain a session"
            );

            throw new IllegalStateException(
                    "Platform did not return a valid session"
            );
        }

        Session session = loginResponse.session();

        /*
         * Reject an already expired session.
         *
         * The Platform must independently validate
         * session expiration and permissions.
         */
        if (session.isExpired()) {

            log.warn(
                    "[LoginController][validatePlatformResponse] "
                            + "Platform returned an expired session"
            );

            throw new IllegalStateException(
                    "Platform returned an expired session"
            );
        }

        /*
         * Never log sessionId or other bearer
         * credentials.
         */
        log.info(() ->
                "[LoginController][validatePlatformResponse] "
                        + "Authentication response validated successfully"
                        + " | username=" + session.username()
                        + " | permissions=" + session.permissions().size()
                        + " | expiresAt=" + session.expiresAt()
        );

        return session;
    }

    // =========================================================
    // User Authentication
    // =========================================================

    /*
     * Creates and submits the authentication request.
     *
     * Unlike a synchronous Session return value,
     * CompletableFuture<Session> represents the
     * result that will eventually arrive from
     * the Platform.
     *
     * The FutureReplyChannel completes loginFuture
     * when the final response arrives.
     */
    private CompletableFuture<Session> validateUser(
            String username,
            char[] password
    ) {

        log.debug(
                "[LoginController][validateUser] "
                        + "Preparing authentication request"
        );

        CompletableFuture<Message> loginFuture =
                new CompletableFuture<>();

        try {

            LoginRequest loginRequest =
                    new LoginRequest(
                            username,
                            password
                    );

            /*
             * Create the standard Mediation Engine
             * message envelope.
             */
            Message loginRequestMessage =
                    messageParser.createEmptyMessage();

            loginRequestMessage.setMessageType(
                    MessageType.REQUEST
            );

            loginRequestMessage.setOperation(
                    Operation.AUTHENTICATE
            );

            loginRequestMessage.setRecipientID(
                    PLATFORM_PROCESS
            );

            /*
             * A correlation ID is required so the
             * TCP protocol processor can associate
             * the Platform response with loginFuture.
             */
            loginRequestMessage.setCorrelationId(
                    UUID.randomUUID()
            );

            /*
             * Jackson converts the strongly typed
             * LoginRequest record into JsonNode.
             */
            loginRequestMessage.setPayload(
                    messageParser.toPayload(loginRequest)
            );

            log.debug(
                    "[LoginController][validateUser] "
                            + "Authentication message created"
                            + " | operation=AUTHENTICATE"
                            + " | recipient=PLATFORM"
            );

            /*
             * Register the FutureReplyChannel before
             * sending the request.
             *
             * The response pipeline eventually invokes
             * FutureReplyChannel.sendReply(), which
             * completes loginFuture.
             */
            FutureReplyChannel replyChannel =
                    new FutureReplyChannel(loginFuture);

            log.debug(
                    "[LoginController][validateUser] "
                            + "FutureReplyChannel initialized"
            );

            /*
             * Submit the request through the existing
             * external TCP communication infrastructure.
             *
             * A successful send does not mean that
             * authentication has succeeded.
             */
            desktopConnectionManager.sendMessage(
                    loginRequestMessage,
                    replyChannel
            ).whenComplete((ignored, sendError) -> {

                if (sendError != null) {

                    log.error(
                            "[LoginController][validateUser] "
                                    + "Authentication request delivery failed",
                            sendError
                    );

                    /*
                     * Ensure callers do not wait indefinitely
                     * when sending itself has failed.
                     */
                    loginFuture.completeExceptionally(
                            sendError
                    );

                } else {

                    log.debug(
                            "[LoginController][validateUser] "
                                    + "Authentication request submitted successfully"
                    );
                }
            });

        } catch (Exception exception) {

            log.error(
                    "[LoginController][validateUser] "
                            + "Unable to prepare or submit authentication request",
                    exception
            );

            loginFuture.completeExceptionally(exception);

        } finally {

            /*
             * Clear the password array supplied by
             * the Swing login page after serialization.
             *
             * Jackson may have created additional
             * temporary copies while encoding JSON.
             */
            Arrays.fill(password, '\0');
        }

        /*
         * Bound the amount of time we wait for the
         * Platform's authentication response.
         *
         * Convert the final response message into
         * a strongly typed Session.
         */
        return loginFuture
                .orTimeout(
                        AUTHENTICATION_TIMEOUT_SECONDS,
                        TimeUnit.SECONDS
                )
                .thenApply(this::validatePlatformResponse);
    }

    // =========================================================
    // Login Handler
    // =========================================================

    private void handleLogin() {

        /*
         * This method executes on Swing's Event Dispatch Thread.
         *
         * Do not perform blocking socket operations here.
         */
        if (!isConnecting.compareAndSet(false, true)) {

            log.debug(
                    "[LoginController][handleLogin] "
                            + "Connection already in progress"
            );

            return;
        }

        /*
         * Read the Swing component values on the EDT.
         *
         * Do not access Swing password or input fields
         * from a TCP worker thread.
         */
        String platformHost = loginPage.getPlatformHost();
        String username     = loginPage.getUsername();
        char[] password     = loginPage.getPassword();

        log.info(() ->
                "[LoginController][handleLogin] Login requested"
                        + " | platformHost=" + platformHost
                        + " | username=" + username
        );

        /*
         * Validate basic input before attempting
         * any network communication.
         */
        if (platformHost == null ||
                platformHost.isBlank() ||
                username == null ||
                username.isBlank() ||
                password == null ||
                password.length == 0) {

            log.warn(
                    "[LoginController][handleLogin] "
                            + "Login rejected because required input is missing"
            );

            if (password != null) {
                Arrays.fill(password, '\0');
            }

            isConnecting.set(false);

            JOptionPane.showMessageDialog(
                    null,
                    "Please enter the Platform address, username and password.",
                    "Missing Login Information",
                    JOptionPane.WARNING_MESSAGE
            );

            return;
        }

        /*
         * Establish the TCP connection asynchronously.
         *
         * The connection manager performs:
         * 1. TCP service initialization.
         * 2. TCP service startup.
         * 3. Platform peer reconciliation.
         * 4. Handshake completion waiting.
         *
         * Once the handshake succeeds, authentication
         * is initiated through validateUser().
         */
        desktopConnectionManager
                .attemptConnectionAsync(platformHost)
                .thenComposeAsync(connected -> {

                    if (!Boolean.TRUE.equals(connected)) {

                        log.warn(
                                "[LoginController][handleLogin] "
                                        + "Platform connection or handshake failed"
                        );

                        return CompletableFuture.failedFuture(
                                new IllegalStateException(
                                        "Unable to establish a connection with the Platform"
                                )
                        );
                    }

                    log.info(
                            "[LoginController][handleLogin] "
                                    + "Platform TCP connection established successfully"
                    );

                    // connected now we proceed with user validation and session generation
                    return validateUser(
                            username,
                            password
                    );

                }, AUTHENTICATION_EXECUTOR)
                .whenCompleteAsync((session, exception) -> {

                    /*
                     * CompletableFuture callbacks may execute on
                     * background threads.
                     *
                     * All Swing UI updates must execute on the EDT.
                     *
                     * This callback is explicitly scheduled
                     * onto the EDT using SwingUtilities::invokeLater.
                     */
                    Arrays.fill(password, '\0');

                    isConnecting.set(false);

                    if (exception != null) {

                        Throwable cause =
                                unwrapException(exception);

                        log.error(
                                "[LoginController][handleLogin] "
                                        + "Login process failed",
                                cause
                        );

                        String errorMessage;

                        if (cause instanceof SecurityException) {

                            errorMessage =
                                    "Authentication was rejected by the Platform.";

                        } else {

                            errorMessage =
                                    "Unable to complete login. Please check the Platform connection and try again.";
                        }

                        JOptionPane.showMessageDialog(
                                null,
                                errorMessage,
                                "Login Failed",
                                JOptionPane.ERROR_MESSAGE
                        );

                        return;
                    }

                    if (session == null) {

                        log.error(
                                "[LoginController][handleLogin] "
                                        + "Login completed without an authenticated session"
                        );

                        JOptionPane.showMessageDialog(
                                null,
                                "The Platform did not return a valid session.",
                                "Login Failed",
                                JOptionPane.ERROR_MESSAGE
                        );

                        return;
                    }

                    /*
                     * Authentication succeeded.
                     *
                     * Store the session so subsequent GUI
                     * operations can use it.
                     */
                    currentSession = session;

                    log.info(() ->
                            "[LoginController][handleLogin] "
                                    + "User authenticated successfully"
                                    + " | username=" + session.username()
                    );

                    /*
                     * Notify the desktop application
                     * that authentication has succeeded.
                     *
                     * The application can now transition
                     * to the dashboard.
                     */
                    try {

                        onAuthenticated.accept(session);

                    } catch (RuntimeException callbackException) {

                        log.error(
                                "[LoginController][handleLogin] "
                                        + "Post-login navigation failed",
                                callbackException
                        );

                        JOptionPane.showMessageDialog(
                                null,
                                "Authentication succeeded, but the dashboard could not be opened.",
                                "Navigation Error",
                                JOptionPane.ERROR_MESSAGE
                        );
                    }

                }, SwingUtilities::invokeLater);
    }

    // =========================================================
    // Exception Utilities
    // =========================================================

    /*
     * CompletableFuture may wrap the original failure
     * inside a CompletionException.
     *
     * Unwrap it so logging and error handling can
     * inspect the actual failure.
     */
    private Throwable unwrapException(
            Throwable exception
    ) {

        Throwable cause = exception;

        while (cause instanceof CompletionException &&
                cause.getCause() != null) {

            cause = cause.getCause();
        }

        return cause;
    }

    // =========================================================
    // Session Access
    // =========================================================

    public Session getCurrentSession() {
        return currentSession;
    }
}
