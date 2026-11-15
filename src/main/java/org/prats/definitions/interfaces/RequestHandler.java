package org.prats.definitions.interfaces;

import org.prats.definitions.classes.Message;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public interface RequestHandler {
    Message handleRequest(Message message);

    default CompletionStage<Message> handleRequestAsync(Message message) {
        try { return CompletableFuture.completedFuture(handleRequest(message)); }
        catch (RuntimeException error) { return CompletableFuture.failedFuture(error); }
    }
}
