package org.prats.desktop;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;
import org.prats.definitions.classes.ProcessContext;
import org.prats.definitions.interfaces.TCPProcessor;
import org.prats.definitions.records.TCPPeer;
import org.prats.services.tcpService.TCPService;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/** Desktop protocol processor. TCPService owns connections, framing, reads and writes. */
public final class PlatformClient implements AutoCloseable {
    private final ObjectMapper json = new ObjectMapper();
    private final ExecutorService connector = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon().name("desktop-platform-connector").factory());
    private final Map<String, CompletableFuture<JsonNode>> pending = new ConcurrentHashMap<>();
    private final Semaphore admission = new Semaphore(32);
    private final java.util.concurrent.atomic.AtomicReference<Connection> current = new java.util.concurrent.atomic.AtomicReference<>();
    private volatile Runnable disconnected = () -> {};
    private volatile boolean closed;

    private final class Connection implements TCPProcessor {
        private final TCPService transport;
        private volatile ClientConnection peer;
        private volatile boolean stopped;

        private Connection() { transport = new TCPService(0, 0, 0, "127.0.0.1", 0, 0, false, this, 2_000_000); }
        @Override public void processIncoming(String raw, ClientConnection connection) throws IOException {
            JsonNode response = json.readTree(raw);
            if (current.get() != this || !response.path("responseComplete").asBoolean(true)) return;
            CompletableFuture<JsonNode> result = pending.remove(response.path("correlationId").asText());
            if (result != null) result.complete(response.path("payload"));
        }
        @Override public String serialize(Message message) { throw new UnsupportedOperationException("Desktop requests use JSON envelopes"); }
        @Override public CompletionStage<Void> processOutgoing(Message message, ClientConnection connection) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("Use request()"));
        }
        @Override public void informShutdown() {}
        @Override public void cleanupCorrelations() {}
        @Override public void reconcilePeers() {}
        @Override public void onConnectionClosed(ClientConnection connection) {
            TCPProcessor.super.onConnectionClosed(connection);
            if (current.compareAndSet(this, null)) {
                failPending();
                disconnected.run();
            }
            stopLater(this);
        }
        private void stop() {
            synchronized (this) {
                if (stopped) return;
                stopped = true;
            }
            transport.shutdown();
        }
    }

    public PlatformClient() {
        try { ProcessContext.getProcessName(); }
        catch (IllegalStateException uninitialized) { ProcessContext.initialize("desktop"); }
    }
    public void onDisconnected(Runnable callback) { disconnected = Objects.requireNonNull(callback); }
    public boolean isConnected() {
        Connection connection = current.get();
        return connection != null && connection.peer != null && !connection.peer.getSocket().isClosed();
    }
    public CompletableFuture<Void> connect(String host, int port) {
        if (closed) return CompletableFuture.failedFuture(new IOException("Desktop client is closed."));
        return CompletableFuture.runAsync(() -> {
            disconnect();
            Connection connection = new Connection();
            current.set(connection);
            try {
                connection.transport.start();
                connection.peer = connection.transport.attemptConnection(new TCPPeer("platform", host, port));
                if (connection.peer == null || current.get() != connection || closed)
                    throw new IOException("Unable to connect to the platform.");
            } catch (Exception error) {
                current.compareAndSet(connection, null);
                connection.stop();
                throw new CompletionException(error);
            }
        }, connector);
    }
    public CompletableFuture<JsonNode> request(String action, Map<String, ?> values) {
        Connection connection = current.get();
        if (!isConnected() || connection == null) return CompletableFuture.failedFuture(new IOException("Connect to the platform first."));
        if (!admission.tryAcquire()) return CompletableFuture.failedFuture(new IOException("Too many pending desktop requests."));
        String correlationId = UUID.randomUUID().toString();
        CompletableFuture<JsonNode> completion = new CompletableFuture<>();
        pending.put(correlationId, completion);
        completion.orTimeout(40, TimeUnit.SECONDS).whenComplete((value, error) -> {
            pending.remove(correlationId, completion);
            admission.release();
        });
        ObjectNode payload = json.valueToTree(values);
        payload.put("action", action);
        try {
            ObjectNode message = json.createObjectNode();
            message.put("messageId", UUID.randomUUID().toString());
            message.put("correlationId", correlationId);
            message.put("messageType", "REQUEST");
            message.put("processType", "GUI");
            message.put("processName", "desktop");
            message.put("operation", "GUI");
            message.put("timestamp", Instant.now().toString());
            message.set("payload", payload);
            if (current.get() != connection) throw new IOException("Platform connection changed.");
            connection.transport.writeMessage(json.writeValueAsString(message), connection.peer)
                    .whenComplete((ignored, error) -> { if (error != null) completion.completeExceptionally(error); });
        } catch (Exception error) { completion.completeExceptionally(error); }
        finally { payload.remove("password"); }
        return completion.thenApply(result -> {
            if (!result.path("ok").asBoolean(false)) throw new CompletionException(new IOException(result.path("body").asText("Request failed.")));
            return result;
        });
    }
    private void failPending() {
        pending.values().forEach(future -> future.completeExceptionally(new IOException("Platform connection closed.")));
        pending.clear();
    }
    public void disconnect() {
        Connection connection = current.getAndSet(null);
        if (connection != null) stopLater(connection);
        failPending();
    }
    private void stopLater(Connection connection) {
        try { connector.execute(connection::stop); }
        catch (RejectedExecutionException ignored) { /* close already queued transport shutdown */ }
    }
    @Override public void close() { closed = true; disconnect(); connector.shutdown(); }
}
