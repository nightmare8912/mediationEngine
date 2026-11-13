//package org.prats.processors;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import org.prats.definitions.classes.ClientConnection;
//import org.prats.definitions.classes.Message;
//import org.prats.definitions.interfaces.ControlMessageHandler;
//import org.prats.loggers.FileLogger;
//import org.prats.parsers.MessageParser;
//import org.prats.definitions.interfaces.ReplyChannel;
//
//import java.util.Map;
//import java.util.Objects;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//import java.util.function.Function;
//
//public class TCPControlProcessor {
//
//    private static final long CORRELATION_TIMEOUT_SECONDS = 60L;
//
//    private record PendingControlRequest(
//            ClientConnection clientConnection,
//            Message originalRequest,
//            CompletableFuture<Message> responseFuture,
//            long expiresAtNanos
//    ) {}
//
//    private final ControlMessageHandler messageHandler;
//    private final MessageParser messageParser;
//
//    private final ConcurrentMap<String, PendingControlRequest> pendingRequests =
//            new ConcurrentHashMap<>();
//
//    public TCPControlProcessor(
//            ControlMessageHandler messageHandler,
//            MessageParser messageParser
//    ) {
//        this.messageHandler = Objects.requireNonNull(
//                messageHandler,
//                "Control message handler cannot be null"
//        );
//
//        this.messageParser = Objects.requireNonNull(
//                messageParser,
//                "Message parser cannot be null"
//        );
//    }
//
//    public CompletableFuture<Message> sendRequest(
//            Message request,
//            ClientConnection clientConnection
//    ) {
//
//        Objects.requireNonNull(request, "Request cannot be null");
//        Objects.requireNonNull(
//                clientConnection,
//                "Client connection cannot be null"
//        );
//
//        String correlationId = getRequiredCorrelationId(request);
//
//        CompletableFuture<Message> responseFuture =
//                new CompletableFuture<>();
//
//        long expiresAtNanos = System.nanoTime()
//                + TimeUnit.SECONDS.toNanos(CORRELATION_TIMEOUT_SECONDS);
//
//        PendingControlRequest pendingRequest = new PendingControlRequest(
//                clientConnection,
//                request,
//                responseFuture,
//                expiresAtNanos
//        );
//
//        PendingControlRequest existingRequest = pendingRequests.putIfAbsent(
//                correlationId,
//                pendingRequest
//        );
//
//        if (existingRequest != null) {
//            throw new IllegalStateException(
//                    "Duplicate control request correlation ID: "
//                            + correlationId
//            );
//        }
//
//        responseFuture.whenComplete((response, exception) -> {
//            if (responseFuture.isCancelled()) {
//                pendingRequests.remove(correlationId, pendingRequest);
//            }
//        });
//
//        try {
//            clientConnection.sendMessage(serializeMessage(request));
//        } catch (Exception exception) {
//            if (pendingRequests.remove(correlationId, pendingRequest)) {
//                responseFuture.completeExceptionally(exception);
//            }
//        }
//        return responseFuture;
//    }
//
//    public void sendResponse(
//            Message response,
//            ClientConnection clientConnection
//    ) throws Exception {
//
//        Objects.requireNonNull(response, "Response cannot be null");
//        Objects.requireNonNull(
//                clientConnection,
//                "Client connection cannot be null"
//        );
//
//        getRequiredCorrelationId(response);
//
//        clientConnection.sendMessage(serializeMessage(response));
//    }
//
//    public void processIncoming(
//            String incomingMessage,
//            ClientConnection clientConnection,
//            Function<Message, ReplyChannel> replyChannelFactory
//    ) throws Exception {
//
//        Objects.requireNonNull(
//                incomingMessage,
//                "Incoming message cannot be null"
//        );
//
//        Objects.requireNonNull(
//                clientConnection,
//                "Client connection cannot be null"
//        );
//
//        Objects.requireNonNull(
//                replyChannelFactory,
//                "Reply channel factory cannot be null"
//        );
//
//        Message message = Objects.requireNonNull(
//                messageParser.createMessageFromJSON(incomingMessage),
//                "Parsed message cannot be null"
//        );
//
//        switch (message.getMessageType()) {
//
//            case REQUEST -> {
//
//                ReplyChannel replyChannel = Objects.requireNonNull(
//                        replyChannelFactory.apply(message),
//                        "Reply channel cannot be null"
//                );
//
//                messageHandler.onRequest(
//                        clientConnection,
//                        message,
//                        replyChannel
//                );
//            }
//
//            case INFO -> processIncomingInfo(message);
//
//            case HEARTBEAT -> processIncomingHeartbeat(message);
//
//            case RESPONSE -> processIncomingResponse(
//                    message,
//                    clientConnection
//            );
//
//            default -> {
//                if (message.getCorrelationId() == null)
//                    break;
//
//                sendResponse(
//                        messageParser.createErrorResponse(
//                                message.getCorrelationId(),
//                                "INVALID_MESSAGE",
//                                "[TCPControlProcessor] [processIncoming] The message was not accepted by the process as it is neither a request nor a response."
//                        ),
//                        clientConnection
//                );
//            }
//
//        }
//    }
//
//    private void processIncomingInfo(Message info) {
//        messageHandler.onInfo(info);
//    }
//
//    private void processIncomingHeartbeat(Message heartbeat) {
//        messageHandler.onHeartbeat(heartbeat);
//    }
//
//    private void processIncomingResponse(
//            Message response,
//            ClientConnection clientConnection
//    ) {
//        String correlationId = getRequiredCorrelationId(response);
//        PendingControlRequest pendingRequest =
//                pendingRequests.get(correlationId);
//        if (pendingRequest == null) {
//            return;
//        }
//        if (pendingRequest.clientConnection() != clientConnection) {
//            return;
//        }
//        if (!pendingRequests.remove(correlationId, pendingRequest)) {
//            return;
//        }
//
//        if (isExpired(pendingRequest)) {
//            pendingRequest.responseFuture().completeExceptionally(
//                    new TimeoutException(
//                            "Control request timed out. Correlation ID: "
//                                    + correlationId
//                    )
//            );
//            return;
//        }
//        pendingRequest.responseFuture().complete(response);
//    }
//
//    public void processAndRemoveExpiredCorrelations() {
//
//        for (Map.Entry<String, PendingControlRequest> entry :
//                pendingRequests.entrySet()) {
//
//            String correlationId = entry.getKey();
//            PendingControlRequest pendingRequest = entry.getValue();
//
//            if (isExpired(pendingRequest)) {
//
//                if (pendingRequests.remove(correlationId, pendingRequest)) {
//
//                    pendingRequest.responseFuture().completeExceptionally(
//                            new TimeoutException(
//                                    "Control request timed out. Correlation ID: "
//                                            + correlationId
//                            )
//                    );
//                }
//            }
//        }
//    }
//
//    public void removePendingRequestsForConnection(
//            ClientConnection clientConnection
//    ) {
//
//        Objects.requireNonNull(
//                clientConnection,
//                "Client connection cannot be null"
//        );
//
//        for (Map.Entry<String, PendingControlRequest> entry :
//                pendingRequests.entrySet()) {
//
//            String correlationId = entry.getKey();
//            PendingControlRequest pendingRequest = entry.getValue();
//
//            if (pendingRequest.clientConnection() == clientConnection) {
//
//                if (pendingRequests.remove(correlationId, pendingRequest)) {
//
//                    pendingRequest.responseFuture().completeExceptionally(
//                            new IllegalStateException(
//                                    "Control connection closed. Correlation ID: "
//                                            + correlationId
//                            )
//                    );
//                }
//            }
//        }
//    }
//
//    public void failAllPendingRequests(Exception cause) {
//
//        Objects.requireNonNull(cause, "Failure cause cannot be null");
//
//        for (Map.Entry<String, PendingControlRequest> entry :
//                pendingRequests.entrySet()) {
//
//            String correlationId = entry.getKey();
//            PendingControlRequest pendingRequest = entry.getValue();
//
//            if (pendingRequests.remove(correlationId, pendingRequest)) {
//                pendingRequest.responseFuture().completeExceptionally(cause);
//            }
//        }
//    }
//
//    private boolean isExpired(PendingControlRequest pendingRequest) {
//
//        return System.nanoTime() - pendingRequest.expiresAtNanos() >= 0L;
//    }
//
//    private String getRequiredCorrelationId(Message message) {
//
//        Object correlationId = message.getCorrelationId();
//
//        if (correlationId == null) {
//            throw new IllegalArgumentException(
//                    "Message correlation ID is required."
//            );
//        }
//
//        String correlationIdString = correlationId.toString();
//
//        if (correlationIdString.isBlank()) {
//            throw new IllegalArgumentException(
//                    "Message correlation ID cannot be blank."
//            );
//        }
//
//        return correlationIdString;
//    }
//
//    private String serializeMessage(Message message) throws Exception {
//        return messageParser.createJSONFromMessage(message);
//    }
//}