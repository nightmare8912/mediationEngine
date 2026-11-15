package org.prats.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.*;
import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.parsers.MessageParser;
import org.prats.replyChannels.FutureReplyChannel;

import java.util.*;
import java.util.concurrent.*;

/** Collects diagnostic subrequests exclusively through MessageService. */
public final class PlatformDiagnostics {
    private final Map<String, String> targets;
    private final MessageDispatcher dispatcher;
    private final MessageParser parser;

    public PlatformDiagnostics(Map<String, String> targets, MessageDispatcher dispatcher, MessageParser parser) {
        this.targets = Map.copyOf(targets);
        this.dispatcher = dispatcher;
        this.parser = parser;
    }

    public CompletableFuture<JsonNode> details(String target, boolean gc) {
        if (!targets.containsKey(target)) return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown JVM: " + target));
        Message request = parser.createEmptyMessage();
        request.setMessageType(MessageType.REQUEST);
        request.setOperation(gc ? Operation.RUN_GC : Operation.DIAGNOSTICS);
        request.setRecipientID(target);
        CompletableFuture<Message> completion = new CompletableFuture<>();
        try { dispatcher.dispatchRequest(request, new FutureReplyChannel(completion)); }
        catch (RuntimeException error) { completion.completeExceptionally(error); }
        return completion.orTimeout(7, TimeUnit.SECONDS).thenApply(response -> {
            JsonNode payload = response.getPayload();
            if (payload == null || !payload.has("heap")) throw new IllegalArgumentException("JVM diagnostics are unavailable: " + target);
            return payload;
        });
    }

    public CompletableFuture<List<Map<String, Object>>> overview() {
        List<CompletableFuture<Map<String, Object>>> pending = new ArrayList<>();
        new TreeMap<>(targets).forEach((id, type) -> pending.add(details(id, false).handle((snapshot, error) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", id);
            row.put("type", type);
            row.put("connected", error == null);
            if (error == null) {
                row.put("status", snapshot.path("status").asText("UNKNOWN"));
                row.put("pid", snapshot.path("pid").asLong());
                row.put("heapUsed", snapshot.path("heap").path("used").asLong());
                row.put("heapMax", snapshot.path("heap").path("max").asLong());
                row.put("uptimeMillis", snapshot.path("uptimeMillis").asLong());
                row.put("machine", snapshot.path("machine"));
                row.put("registries", snapshot.path("registries"));
                row.put("services", snapshot.path("services"));
                row.put("connections", snapshot.path("connections"));
            } else {
                row.put("status", "UNREACHABLE");
                row.put("body", "JVM diagnostics are unavailable.");
            }
            return row;
        })));
        return CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> pending.stream().map(future -> future.getNow(null)).toList());
    }
}
