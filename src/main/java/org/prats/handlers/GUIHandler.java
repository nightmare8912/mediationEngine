package org.prats.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.*;
import org.prats.definitions.interfaces.AsyncRequestHandler;
import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.parsers.MessageParser;
import org.prats.replyChannels.FutureReplyChannel;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

/** GUI orchestration and formatting. All subrequests enter MessageService with their own correlation. */
public final class GUIHandler implements AsyncRequestHandler {
    private final MessageDispatcher dispatcher;
    private final MessageParser parser;
    private final Supplier<List<String>> containers;
    private PlatformDiagnostics diagnostics;

    public GUIHandler(MessageDispatcher dispatcher, MessageParser parser, Supplier<List<String>> containers) {
        this.dispatcher = Objects.requireNonNull(dispatcher);
        this.parser = parser;
        this.containers = containers;
    }
    public void setDiagnostics(PlatformDiagnostics diagnostics) { this.diagnostics = diagnostics; }

    @Override public CompletionStage<Message> handleRequestAsync(Message request) {
        org.prats.loggers.EngineLogger.getLogger(GUIHandler.class).debug(() ->
                "[GUIHandler][handleRequestAsync] GUI request received | correlationId=" + request.getCorrelationId());
        JsonNode payload = request.getPayload();
        String action = payload == null ? "" : payload.path("action").asText("");
        CompletionStage<JsonNode> result;
        try {
            Objects.requireNonNull(request.getClientSession(), "Client session is unavailable.");
            if (Set.of("register", "login", "logout", "whoami").contains(action)) {
                result = authenticate(request, action);
            } else if (action.equals("application") && isHelp(request.getApplicationRequest())) {
                result = execute(request, action);
            } else {
                result = authenticate(request, "authorize").thenCompose(ignored -> execute(request, action));
            }
        } catch (RuntimeException error) { result = CompletableFuture.failedFuture(error); }
        // Authentication owns its copy; do not retain credentials on the original GUI envelope.
        if (payload instanceof ObjectNode object) object.remove("password");
        request.setFullStringOriginalMessage(null);
        return result.handle((body, error) -> {
            ObjectNode formatted = error == null ? (ObjectNode) body.deepCopy()
                    : (ObjectNode) parser.convertToJsonNode(Map.of("ok", false, "body", reason(error)));
            if (error == null && !formatted.has("ok")) formatted.put("ok", true);
            Message response = parser.createResponse(request.getCorrelationId(), Map.of());
            response.setPayload(formatted);
            response.setOperation(request.getApplicationRequest() != null
                    ? responseOperation(request.getApplicationRequest())
                    : payload != null && payload.path("textReply").asBoolean(false) ? Operation.COMMAND : Operation.GUI);
            response.setStatus(formatted.path("ok").asBoolean(false) ? "SUCCESS" : "FAILED");
            response.setResponseComplete(true);
            return response;
        });
    }

    private CompletionStage<JsonNode> authenticate(Message original, String action) {
        Message request = parser.createEmptyMessage();
        request.setMessageType(MessageType.REQUEST);
        request.setOperation(Operation.AUTHENTICATE);
        ObjectNode payload = original.getPayload() == null ? new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode()
                : (ObjectNode) original.getPayload().deepCopy();
        payload.put("action", action);
        request.setPayload(payload);
        request.setClientSession(original.getClientSession());
        return dispatch(request, 20).thenApply(response -> {
            JsonNode result = response.getPayload();
            if (result == null || !result.path("ok").asBoolean(false))
                throw new IllegalArgumentException(result == null ? "Authentication is unavailable." : result.path("body").asText("Login required."));
            return result;
        });
    }

    private CompletionStage<JsonNode> execute(Message original, String action) {
        JsonNode payload = original.getPayload();
        return switch (action) {
            case "containers" -> CompletableFuture.completedFuture(parser.convertToJsonNode(
                    Map.of("containers", containers.get(), "body", "Configured execution containers.")));
            case "overview" -> Objects.requireNonNull(diagnostics, "Diagnostics unavailable").overview()
                    .thenApply(rows -> parser.convertToJsonNode(Map.of("jvms", rows, "body", "JVM monitoring updated.")));
            case "details", "gc" -> Objects.requireNonNull(diagnostics, "Diagnostics unavailable")
                    .details(payload.path("target").asText(""), action.equals("gc"))
                    .thenApply(details -> parser.convertToJsonNode(Map.of("details", details, "body", action.equals("gc")
                            ? "GC requested; the JVM may ignore the request." : "Live JVM diagnostics.")));
            case "command" -> {
                String command = payload.path("command").asText("").trim().replaceAll("\\s+", " ");
                if (!command.matches("(?i)mng (help|status|health report [a-zA-Z0-9][a-zA-Z0-9._-]*|(start|stop|restart) ec [a-zA-Z0-9][a-zA-Z0-9._-]*)"))
                    throw new IllegalArgumentException("Unsupported desktop command. Use mng help for supported operations.");
                Message commandRequest = parser.createMessageFromJSON(command);
                commandRequest.generateRandomCorrelationId();
                yield dispatch(commandRequest, 30).thenApply(response -> {
                    JsonNode result = response.getPayload();
                    ObjectNode formatted = result != null && result.isObject() ? (ObjectNode) result.deepCopy()
                            : (ObjectNode) parser.convertToJsonNode(Map.of("body", "No response body."));
                    formatted.put("ok", !"FAILED".equals(response.getStatus()));
                    return formatted;
                });
            }
            case "application" -> {
                Message application = Objects.requireNonNull(original.getApplicationRequest(), "Application request unavailable.");
                // An external caller cannot enter the internal account or diagnostic routes.
                if (Set.of(Operation.AUTHENTICATE, Operation.DIAGNOSTICS, Operation.RUN_GC).contains(application.getOperation()))
                    throw new IllegalArgumentException("Use GUI actions for authentication and diagnostics.");
                if (application.getMessageType() != MessageType.REQUEST
                        && !(application.getMessageType() == MessageType.UNKNOWN && application.getOperation() == Operation.UNKNOWN))
                    throw new IllegalArgumentException("Expected an application request.");
                application.generateRandomCorrelationId();
                application.setClientSession(null);
                yield dispatch(application, 30).thenApply(response -> {
                    ObjectNode result = response.getPayload() != null && response.getPayload().isObject()
                            ? (ObjectNode) response.getPayload().deepCopy()
                            : (ObjectNode) parser.convertToJsonNode(Map.of("body", "No response body."));
                    if (!result.has("ok")) result.put("ok", !"FAILED".equals(response.getStatus()));
                    return result;
                });
            }
            default -> throw new IllegalArgumentException("Unknown GUI action.");
        };
    }

    private CompletableFuture<Message> dispatch(Message request, int seconds) {
        CompletableFuture<Message> completion = new CompletableFuture<>();
        try { dispatcher.dispatchRequest(request, new FutureReplyChannel(completion)); }
        catch (RuntimeException error) { completion.completeExceptionally(error); }
        return completion.orTimeout(seconds, TimeUnit.SECONDS);
    }
    private static boolean isHelp(Message message) {
        return message != null && message.getOperation() == Operation.UNKNOWN
                && "mng help".equalsIgnoreCase(Objects.toString(message.getFullStringOriginalMessage(), "").trim().replaceAll("\\s+", " "));
    }
    private static Operation responseOperation(Message message) {
        return message.getOperation() == Operation.UNKNOWN ? Operation.COMMAND : message.getOperation();
    }
    private static String reason(Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException) && error.getCause() != null) error = error.getCause();
        if (error instanceof IllegalArgumentException) return error.getMessage();
        if (error instanceof TimeoutException) return "Operation timed out; check EC state before retrying.";
        return "The platform could not complete this request. Check service logs.";
    }
}
