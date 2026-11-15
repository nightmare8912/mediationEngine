package org.prats.forwarders;

import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.*;
import org.prats.parsers.MessageParser;
import org.prats.processors.TCPControlProcessor;
import org.prats.replyChannels.FutureReplyChannel;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/** Message route for local diagnostic execution or remote control-channel delivery. */
public final class DiagnosticsForwarder implements Forwarder {
    private final String localId;
    private final Set<String> targets;
    private final Submittable local;
    private final TCPControlProcessor control;
    private final MessageDispatcher dispatcher;
    private final MessageParser parser;

    public DiagnosticsForwarder(String localId, Set<String> targets, Submittable local,
                                TCPControlProcessor control, MessageDispatcher dispatcher, MessageParser parser) {
        this.localId = localId;
        this.targets = Set.copyOf(targets);
        this.local = local;
        this.control = control;
        this.dispatcher = dispatcher;
        this.parser = parser;
    }
    @Override public void forward(Message request) {
        if (!targets.contains(request.getRecipientID())) throw new IllegalArgumentException("Unknown JVM target");
        if (localId.equals(request.getRecipientID())) { local.submit(request); return; }
        CompletableFuture<Message> completion = new CompletableFuture<>();
        completion.orTimeout(5, TimeUnit.SECONDS).whenComplete((response, error) -> {
            if (error == null) dispatcher.dispatchResponse(response);
            else {
                Message failure = parser.createResponse(request.getCorrelationId(), Map.of());
                failure.setOperation(request.getOperation());
                failure.setPayload(parser.convertToJsonNode(Map.of("ok", false, "body", "JVM is disconnected or unavailable.")));
                failure.setStatus("FAILED");
                dispatcher.dispatchResponse(failure);
            }
        });
        try {
            control.sendRequest(request, new FutureReplyChannel(completion)).whenComplete((ignored, error) -> {
                if (error != null) completion.completeExceptionally(error);
            });
        } catch (RuntimeException error) { completion.completeExceptionally(error); }
    }
}
