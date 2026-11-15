
package org.prats.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.AsyncRequestHandler;

import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;
import org.prats.security.AccountManager;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

/**
 * Handles authentication requests originating inside the platform.
 *
 * All account requests carry a transport-local session context.
 * Authentication policies and account state are managed by AccountManager.
 */
public final class AuthenticationHandler implements AsyncRequestHandler {

    private static final EngineLogger log =
            EngineLogger.getLogger(AuthenticationHandler.class);

    private final AccountManager accounts;
    private final MessageParser parser;


    // =========================================================
    // 1. CONSTRUCTOR
    // =========================================================

    public AuthenticationHandler(
            AccountManager accounts,
            MessageParser parser
    ) {

        this.accounts = Objects.requireNonNull(accounts);
        this.parser = Objects.requireNonNull(parser);

        log.debug(() ->
                "[AuthenticationHandler][constructor] Authentication handler initialized"
        );
    }


    // =========================================================
    // 2. ASYNCHRONOUS REQUEST HANDLING
    // =========================================================

    @Override
    public CompletionStage<Message> handleRequestAsync(
            Message request
    ) {

        log.debug(() ->
                "[AuthenticationHandler][handleRequestAsync] Authentication request received"
                        + " | correlationId=" + request.getCorrelationId()
        );

        CompletionStage<Map<String, ?>> result;

        var payload = request.getPayload();

        char[] password = payload == null
                ? new char[0]
                : payload.path("password")
                .asText("")
                .toCharArray();

        try {

            var context = Objects.requireNonNull(
                    request.getClientSession(),
                    "Client session is unavailable."
            );

            String action, username;

            try {
                action      = parser.extractPayloadField(request, "action", String.class);
                username    = parser.extractPayloadField(request, "username", String.class);

            } catch (JsonProcessingException jsonProcessingException) {

                log.error(
                        "[AuthenticationHandler] [handleRequestAsync] Error while authenticating payload fields: " + jsonProcessingException
                );

                throw new IllegalArgumentException(
                        jsonProcessingException
                );
            }

            log.debug(() ->
                    "[AuthenticationHandler][handleRequestAsync] Processing authentication action"
                            + " | action=" + action
            );

            result = switch (action) {


                // -------------------------------------------------
                // 2.1. Account registration
                // -------------------------------------------------

                case "register" -> {

                    log.debug(() ->
                            "[AuthenticationHandler][register] Account registration requested"
                    );

                    yield accounts.register(
                            username,
                            password
                    ).thenApply(ignored ->
                            Map.of(
                                    "body",
                                    "User registered. Log in with mng login <username> <password>."
                            )
                    );
                }


                // -------------------------------------------------
                // 2.2. Account login
                // -------------------------------------------------

                case "login" -> {

                    log.debug(() ->
                            "[AuthenticationHandler][login] Account login requested"
                    );

                    yield accounts.login(
                            username,
                            password
                    ).thenApply(session -> {

                        synchronized (context) {

                            if (context.isClosed()) {

                                accounts.logout(session.token());

                                throw new IllegalArgumentException(
                                        "Client connection is closed."
                                );
                            }

                            accounts.logout(context.token());

                            context.authenticate(session.token());
                        }

                        log.info(() ->
                                "[AuthenticationHandler][login] Account login completed"
                                        + " | username=" + session.username()
                        );

                        return Map.of(
                                "username", session.username(),
                                "body", "Logged in as " + session.username() + "."
                        );
                    });
                }


                // -------------------------------------------------
                // 2.3. Logout and disconnect
                // -------------------------------------------------

                case "logout", "disconnect" -> {

                    log.debug(() ->
                            "[AuthenticationHandler][logout] Logout requested"
                                    + " | action=" + action
                    );

                    synchronized (context) {

                        accounts.logout(context.token());

                        context.clear();
                    }

                    log.info(
                            "[AuthenticationHandler][logout] Client session cleared"
                    );

                    yield CompletableFuture.completedFuture(
                            Map.of(
                                    "body",
                                    "Logged out."
                            )
                    );
                }


                // -------------------------------------------------
                // 2.4. Authorization and current user lookup
                // -------------------------------------------------

                case "authorize", "whoami" -> {

                    log.debug(() ->
                            "[AuthenticationHandler][authorize] Session authorization requested"
                                    + " | action=" + action
                    );

                    if (context.isClosed()) {

                        throw new IllegalArgumentException(
                                "Client connection is closed."
                        );
                    }

                    var session = accounts.session(
                            context.token()
                    ).orElseThrow(() ->
                            new IllegalArgumentException(
                                    "Login required. Use mng login <username> <password>."
                            )
                    );

                    log.debug(() ->
                            "[AuthenticationHandler][authorize] Session authorized successfully"
                                    + " | username=" + session.username()
                    );

                    yield CompletableFuture.completedFuture(
                            Map.of(
                                    "username", session.username(),
                                    "body", "Logged in as " + session.username() + "."
                            )
                    );
                }


                // -------------------------------------------------
                // 2.5. Unsupported authentication action
                // -------------------------------------------------

                default -> {

                    log.warn(
                            "[AuthenticationHandler][handleRequestAsync] Unsupported authentication action"
                                    + " | action=" + action
                    );

                    throw new IllegalArgumentException(
                            "Unknown authentication action."
                    );
                }
            };

        } catch (RuntimeException error) {

            log.debug(() ->
                    "[AuthenticationHandler][handleRequestAsync] Authentication request failed"
                            + " | exception=" + error.getClass().getSimpleName()
            );

            result = CompletableFuture.failedFuture(error);
        }


        // -----------------------------------------------------
        // 2.6. Remove credentials from the original message
        // -----------------------------------------------------

        /*
         * The password array remains available until asynchronous
         * verification has finished.
         */

        if (payload instanceof ObjectNode object) {
            object.remove("password");
        }

        request.setFullStringOriginalMessage(null);


        // -----------------------------------------------------
        // 2.7. Format the authentication response
        // -----------------------------------------------------

        return result.handle((values, error) -> {

            // Clear password characters after processing.
            Arrays.fill(password, '\0');

            Message response = parser.createResponse(
                    request.getCorrelationId(),
                    Map.of()
            );

            ObjectNode body = parser.convertToJsonNode(
                    error == null
                            ? values
                            : Map.of("body", reason(error))
            ).deepCopy();

            body.put(
                    "ok",
                    error == null
            );

            response.setPayload(body);

            response.setOperation(
                    request.getOperation()
            );

            response.setStatus(
                    error == null
                            ? "SUCCESS"
                            : "FAILED"
            );

            response.setResponseComplete(true);

            log.debug(() ->
                    "[AuthenticationHandler][handleRequestAsync] Authentication response prepared"
                            + " | correlationId=" + request.getCorrelationId()
                            + " | status=" + response.getStatus()
            );

            return response;
        });
    }


    // =========================================================
    // 3. ERROR MESSAGE RESOLUTION
    // =========================================================

    private String reason(Throwable error) {

        while ((error instanceof CompletionException
                || error instanceof ExecutionException)
                && error.getCause() != null) {

            error = error.getCause();
        }

        if (error instanceof IllegalArgumentException) {

            return error.getMessage();
        }

        return "Authentication is unavailable. Try again later.";
    }
}
