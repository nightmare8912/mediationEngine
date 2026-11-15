package org.prats.handlers;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.AsyncRequestHandler;
import org.prats.parsers.MessageParser;
import org.prats.security.AccountManager;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

/** Account requests originate inside the platform and carry a transport-local session context. */
public final class AuthenticationHandler implements AsyncRequestHandler {
    private final AccountManager accounts;
    private final MessageParser parser;

    public AuthenticationHandler(AccountManager accounts, MessageParser parser) {
        this.accounts = accounts;
        this.parser = parser;
    }

    @Override public CompletionStage<Message> handleRequestAsync(Message request) {
        CompletionStage<Map<String, ?>> result;
        var payload = request.getPayload();
        char[] password = payload == null ? new char[0] : payload.path("password").asText("").toCharArray();
        try {
            var context = Objects.requireNonNull(request.getClientSession(), "Client session is unavailable.");
            String action = payload.path("action").asText("");
            String username = payload.path("username").asText("");
            result = switch (action) {
                case "register" -> accounts.register(username, password).thenApply(ignored ->
                        Map.of("body", "User registered. Log in with mng login <username> <password>."));
                case "login" -> accounts.login(username, password).thenApply(session -> {
                    synchronized (context) {
                        if (context.isClosed()) {
                            accounts.logout(session.token());
                            throw new IllegalArgumentException("Client connection is closed.");
                        }
                        accounts.logout(context.token());
                        context.authenticate(session.token());
                    }
                    return Map.of("username", session.username(), "body", "Logged in as " + session.username() + ".");
                });
                case "logout", "disconnect" -> {
                    synchronized (context) {
                        accounts.logout(context.token());
                        context.clear();
                    }
                    yield CompletableFuture.completedFuture(Map.of("body", "Logged out."));
                }
                case "authorize", "whoami" -> {
                    if (context.isClosed()) throw new IllegalArgumentException("Client connection is closed.");
                    var session = accounts.session(context.token()).orElseThrow(() ->
                            new IllegalArgumentException("Login required. Use mng login <username> <password>."));
                    yield CompletableFuture.completedFuture(Map.of("username", session.username(),
                            "body", "Logged in as " + session.username() + "."));
                }
                default -> throw new IllegalArgumentException("Unknown authentication action.");
            };
        } catch (RuntimeException error) { result = CompletableFuture.failedFuture(error); }
        // The password array remains available until asynchronous verification has finished.
        if (payload instanceof ObjectNode object) object.remove("password");
        request.setFullStringOriginalMessage(null);
        return result.handle((values, error) -> {
            Arrays.fill(password, '\0');
            Message response = parser.createResponse(request.getCorrelationId(), Map.of());
            ObjectNode body = parser.convertToJsonNode(error == null ? values : Map.of("body", reason(error))).deepCopy();
            body.put("ok", error == null);
            response.setPayload(body);
            response.setOperation(request.getOperation());
            response.setStatus(error == null ? "SUCCESS" : "FAILED");
            response.setResponseComplete(true);
            return response;
        });
    }

    private String reason(Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException) && error.getCause() != null)
            error = error.getCause();
        return error instanceof IllegalArgumentException ? error.getMessage() : "Authentication is unavailable. Try again later.";
    }
}
