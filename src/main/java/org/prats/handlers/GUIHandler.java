
package org.prats.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.interfaces.AsyncRequestHandler;
import org.prats.definitions.interfaces.MessageDispatcher;

import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;
import org.prats.replyChannels.FutureReplyChannel;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import java.util.function.Supplier;

/**
 * GUI orchestration and response formatting.
 *
 * All subrequests enter MessageService with their
 * own correlation identifiers.
 */
public final class GUIHandler implements AsyncRequestHandler {

    private static final EngineLogger log =
            EngineLogger.getLogger(GUIHandler.class);

    private final MessageDispatcher dispatcher;
    private final MessageParser parser;
    private final Supplier<List<String>> containers;

    private PlatformDiagnostics diagnostics;


    // =========================================================
    // 1. CONSTRUCTOR
    // =========================================================

    public GUIHandler(
            MessageDispatcher dispatcher,
            MessageParser parser,
            Supplier<List<String>> containers
    ) {

        this.dispatcher = Objects.requireNonNull(dispatcher);
        this.parser = parser;
        this.containers = containers;

        log.debug(() ->
                "[GUIHandler][constructor] GUI handler initialized"
        );
    }


    // =========================================================
    // 2. DIAGNOSTICS DEPENDENCY INJECTION
    // =========================================================

    public void setDiagnostics(PlatformDiagnostics diagnostics) {

        this.diagnostics = diagnostics;

        log.debug(() ->
                "[GUIHandler][setDiagnostics] Platform diagnostics configured"
                        + " | configured=" + (diagnostics != null)
        );
    }


    // =========================================================
    // 3. ASYNCHRONOUS GUI REQUEST HANDLING
    // =========================================================

    @Override
    public CompletionStage<Message> handleRequestAsync(
            Message request
    ) {

        log.debug(() ->
                "[GUIHandler][handleRequestAsync] GUI request received"
                        + " | correlationId=" + request.getCorrelationId()
        );

        JsonNode payload = request.getPayload();

        String action = payload == null
                ? ""
                : payload.path("action").asText("");

        CompletionStage<JsonNode> result;


        // -----------------------------------------------------
        // 3.1. Validate session and route request
        // -----------------------------------------------------

        try {

            Objects.requireNonNull(
                    request.getClientSession(),
                    "Client session is unavailable."
            );

            if (Set.of(
                    "register",
                    "login",
                    "logout",
                    "whoami"
            ).contains(action)) {

                result = authenticate(
                        request,
                        action
                );

            } else if (action.equals("application")
                    && isHelp(request.getApplicationRequest())) {

                result = execute(
                        request,
                        action
                );

            } else {

                result = authenticate(
                        request,
                        "authorize"
                ).thenCompose(
                        ignored -> execute(request, action)
                );
            }

        } catch (RuntimeException error) {

            result = CompletableFuture.failedFuture(error);
        }


        // -----------------------------------------------------
        // 3.2. Remove credentials from original GUI envelope
        // -----------------------------------------------------

        /*
         * Authentication owns its copy.
         * Do not retain credentials on the original GUI envelope.
         */

        if (payload instanceof ObjectNode object) {
            object.remove("password");
        }

        request.setFullStringOriginalMessage(null);


        // -----------------------------------------------------
        // 3.3. Format asynchronous response
        // -----------------------------------------------------

        return result.handle((body, error) -> {

            ObjectNode formatted = error == null
                    ? (ObjectNode) body.deepCopy()
                    : (ObjectNode) parser.convertToJsonNode(
                    Map.of(
                            "ok", false,
                            "body", reason(error)
                    )
            );


            // ---------------------------------------------
            // Add success status when not already supplied
            // ---------------------------------------------

            if (error == null && !formatted.has("ok")) {
                formatted.put("ok", true);
            }


            // ---------------------------------------------
            // Create response message
            // ---------------------------------------------

            Message response = parser.createResponse(
                    request.getCorrelationId(),
                    Map.of()
            );

            response.setPayload(formatted);


            // ---------------------------------------------
            // Resolve response operation
            // ---------------------------------------------

            response.setOperation(
                    request.getApplicationRequest() != null

                            ? responseOperation(
                            request.getApplicationRequest()
                    )

                            : payload != null
                            && payload.path("textReply")
                            .asBoolean(false)

                            ? Operation.COMMAND
                            : Operation.GUI
            );


            // ---------------------------------------------
            // Set response status
            // ---------------------------------------------

            response.setStatus(
                    formatted.path("ok").asBoolean(false)
                            ? "SUCCESS"
                            : "FAILED"
            );

            response.setResponseComplete(true);

            log.debug(() ->
                    "[GUIHandler][handleRequestAsync] GUI response formatted"
                            + " | correlationId=" + request.getCorrelationId()
                            + " | action=" + action
                            + " | status=" + response.getStatus()
            );

            return response;
        });
    }


    // =========================================================
    // 4. AUTHENTICATION REQUEST DISPATCH
    // =========================================================

    private CompletionStage<JsonNode> authenticate(
            Message original,
            String action
    ) {

        log.debug(() ->
                "[GUIHandler][authenticate] Authentication request initiated"
                        + " | action=" + action
                        + " | correlationId=" + original.getCorrelationId()
        );

        Message request = parser.createEmptyMessage();

        request.setMessageType(MessageType.REQUEST);
        request.setOperation(Operation.AUTHENTICATE);


        // -----------------------------------------------------
        // 4.1. Copy original request payload
        // -----------------------------------------------------

        ObjectNode payload = original.getPayload() == null
                ? new ObjectMapper().createObjectNode()
                : (ObjectNode) original.getPayload().deepCopy();

        payload.put("action", action);

        request.setPayload(payload);

        request.setClientSession(
                original.getClientSession()
        );


        // -----------------------------------------------------
        // 4.2. Dispatch authentication subrequest
        // -----------------------------------------------------

        return dispatch(request, 20)
                .thenApply(response -> {

                    JsonNode result = response.getPayload();

                    if (result == null
                            || !result.path("ok").asBoolean(false)) {

                        throw new IllegalArgumentException(
                                result == null
                                        ? "Authentication is unavailable."
                                        : result.path("body")
                                        .asText("Login required.")
                        );
                    }

                    log.debug(() ->
                            "[GUIHandler][authenticate] Authentication request completed"
                                    + " | action=" + action
                    );

                    return result;
                });
    }


    // =========================================================
    // 5. GUI ACTION EXECUTION
    // =========================================================

    private CompletionStage<JsonNode> execute(
            Message original,
            String action
    ) {

        JsonNode payload = original.getPayload();

        log.debug(() ->
                "[GUIHandler][execute] Executing GUI action"
                        + " | action=" + action
                        + " | correlationId=" + original.getCorrelationId()
        );

        return switch (action) {


            // -------------------------------------------------
            // 5.1. List configured execution containers
            // -------------------------------------------------

            case "containers" -> {

                log.debug(() ->
                        "[GUIHandler][execute] Retrieving configured execution containers"
                );

                yield CompletableFuture.completedFuture(
                        parser.convertToJsonNode(
                                Map.of(
                                        "containers", containers.get(),
                                        "body", "Configured execution containers."
                                )
                        )
                );
            }


            // -------------------------------------------------
            // 5.2. Retrieve platform monitoring overview
            // -------------------------------------------------

            case "overview" -> {

                log.debug(() ->
                        "[GUIHandler][execute] Retrieving JVM monitoring overview"
                );

                yield Objects.requireNonNull(
                        diagnostics,
                        "Diagnostics unavailable"
                ).overview().thenApply(rows ->
                        parser.convertToJsonNode(
                                Map.of(
                                        "jvms", rows,
                                        "body", "JVM monitoring updated."
                                )
                        )
                );
            }


            // -------------------------------------------------
            // 5.3. Retrieve JVM diagnostics or request GC
            // -------------------------------------------------

            case "details", "gc" -> {

                String target = payload.path("target").asText("");

                boolean requestGC = action.equals("gc");

                log.debug(() ->
                        "[GUIHandler][execute] JVM diagnostics requested"
                                + " | target=" + target
                                + " | requestGC=" + requestGC
                );

                yield Objects.requireNonNull(
                        diagnostics,
                        "Diagnostics unavailable"
                ).details(
                        target,
                        requestGC
                ).thenApply(details ->
                        parser.convertToJsonNode(
                                Map.of(
                                        "details", details,
                                        "body", requestGC
                                                ? "GC requested; the JVM may ignore the request."
                                                : "Live JVM diagnostics."
                                )
                        )
                );
            }


            // -------------------------------------------------
            // 5.4. Execute supported desktop commands
            // -------------------------------------------------

            case "command" -> {

                String command = payload.path("command")
                        .asText("")
                        .trim()
                        .replaceAll("\\s+", " ");


                // ---------------------------------------------
                // Validate supported commands
                // ---------------------------------------------

                if (!command.matches(
                        "(?i)mng (help|status|health report "
                                + "[a-zA-Z0-9][a-zA-Z0-9._-]*"
                                + "|(start|stop|restart) ec "
                                + "[a-zA-Z0-9][a-zA-Z0-9._-]*)"
                )) {

                    log.warn(
                            "[GUIHandler][execute] Unsupported desktop command rejected"
                    );

                    throw new IllegalArgumentException(
                            "Unsupported desktop command. "
                                    + "Use mng help for supported operations."
                    );
                }

                log.debug(() ->
                        "[GUIHandler][execute] Dispatching desktop command"
                                + " | command=" + command
                );


                // ---------------------------------------------
                // Parse command and generate correlation
                // ---------------------------------------------

                Message commandRequest =
                        parser.createMessageFromJSON(command);

                commandRequest.generateRandomCorrelationId();


                // ---------------------------------------------
                // Dispatch command request
                // ---------------------------------------------

                yield dispatch(commandRequest, 30)
                        .thenApply(response -> {

                            JsonNode result = response.getPayload();

                            ObjectNode formatted =
                                    result != null && result.isObject()

                                            ? (ObjectNode) result.deepCopy()

                                            : (ObjectNode) parser.convertToJsonNode(
                                            Map.of(
                                                    "body",
                                                    "No response body."
                                            )
                                    );

                            formatted.put(
                                    "ok",
                                    !"FAILED".equals(response.getStatus())
                            );

                            return formatted;
                        });
            }


            // -------------------------------------------------
            // 5.5. Execute application requests
            // -------------------------------------------------

            case "application" -> {

                Message application = Objects.requireNonNull(
                        original.getApplicationRequest(),
                        "Application request unavailable."
                );


                // ---------------------------------------------
                // Prevent access to internal routes
                // ---------------------------------------------

                /*
                 * An external caller cannot enter the internal
                 * account or diagnostic routes.
                 */

                if (Set.of(
                        Operation.AUTHENTICATE,
                        Operation.DIAGNOSTICS,
                        Operation.RUN_GC
                ).contains(application.getOperation())) {

                    log.warn(
                            "[GUIHandler][execute] Application request rejected"
                                    + " | reason=RESTRICTED_OPERATION"
                    );

                    throw new IllegalArgumentException(
                            "Use GUI actions for authentication and diagnostics."
                    );
                }


                // ---------------------------------------------
                // Validate application message type
                // ---------------------------------------------

                if (application.getMessageType() != MessageType.REQUEST
                        && !(application.getMessageType() == MessageType.UNKNOWN
                        && application.getOperation() == Operation.UNKNOWN)) {

                    throw new IllegalArgumentException(
                            "Expected an application request."
                    );
                }


                // ---------------------------------------------
                // Prepare application request
                // ---------------------------------------------

                application.generateRandomCorrelationId();

                application.setClientSession(null);

                log.debug(() ->
                        "[GUIHandler][execute] Dispatching application request"
                                + " | correlationId=" + application.getCorrelationId()
                                + " | operation=" + application.getOperation()
                );


                // ---------------------------------------------
                // Dispatch application request
                // ---------------------------------------------

                yield dispatch(application, 30)
                        .thenApply(response -> {

                            ObjectNode result =
                                    response.getPayload() != null
                                            && response.getPayload().isObject()

                                            ? (ObjectNode) response
                                            .getPayload()
                                            .deepCopy()

                                            : (ObjectNode) parser.convertToJsonNode(
                                            Map.of(
                                                    "body",
                                                    "No response body."
                                            )
                                    );

                            if (!result.has("ok")) {

                                result.put(
                                        "ok",
                                        !"FAILED".equals(
                                                response.getStatus()
                                        )
                                );
                            }

                            return result;
                        });
            }


            // -------------------------------------------------
            // 5.6. Reject unknown GUI actions
            // -------------------------------------------------

            default -> {

                log.warn(
                        "[GUIHandler][execute] Unknown GUI action rejected"
                );

                throw new IllegalArgumentException(
                        "Unknown GUI action."
                );
            }
        };
    }


    // =========================================================
    // 6. INTERNAL MESSAGE DISPATCH
    // =========================================================

    private CompletableFuture<Message> dispatch(
            Message request,
            int seconds
    ) {

        log.debug(() ->
                "[GUIHandler][dispatch] Dispatching internal request"
                        + " | correlationId=" + request.getCorrelationId()
                        + " | operation=" + request.getOperation()
                        + " | timeoutSeconds=" + seconds
        );

        CompletableFuture<Message> completion =
                new CompletableFuture<>();

        try {

            dispatcher.dispatchRequest(
                    request,
                    new FutureReplyChannel(completion)
            );

        } catch (RuntimeException error) {

            log.error(
                    "[GUIHandler][dispatch] Internal request dispatch failed"
                            + " | correlationId=" + request.getCorrelationId(),
                    error
            );

            completion.completeExceptionally(error);
        }

        return completion.orTimeout(
                seconds,
                TimeUnit.SECONDS
        );
    }


    // =========================================================
    // 7. HELP COMMAND DETECTION
    // =========================================================

    private static boolean isHelp(Message message) {

        return message != null
                && message.getOperation() == Operation.UNKNOWN
                && "mng help".equalsIgnoreCase(
                Objects.toString(
                                message.getFullStringOriginalMessage(),
                                ""
                        )
                        .trim()
                        .replaceAll("\\s+", " ")
        );
    }


    // =========================================================
    // 8. RESPONSE OPERATION RESOLUTION
    // =========================================================

    private static Operation responseOperation(Message message) {

        return message.getOperation() == Operation.UNKNOWN
                ? Operation.COMMAND
                : message.getOperation();
    }


    // =========================================================
    // 9. ERROR MESSAGE RESOLUTION
    // =========================================================

    private static String reason(Throwable error) {

        while ((error instanceof CompletionException
                || error instanceof ExecutionException)
                && error.getCause() != null) {

            error = error.getCause();
        }

        if (error instanceof IllegalArgumentException) {

            return error.getMessage();
        }

        if (error instanceof TimeoutException) {

            return "Operation timed out; check EC state before retrying.";
        }

        return "The platform could not complete this request. "
                + "Check service logs.";
    }
}
