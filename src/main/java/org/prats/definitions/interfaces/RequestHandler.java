
package org.prats.definitions.interfaces;

import org.prats.definitions.classes.Message;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Defines the contract for handling requests.
 *
 * Supports both synchronous and asynchronous execution.
 * Implementations may override handleRequestAsync() to
 * provide genuinely asynchronous processing.
 */
public interface RequestHandler {

    /**
     * Handles a request synchronously.
     *
     * @param message the incoming request
     * @return the response message
     */
    Message handleRequest(Message message);

    /**
     * Provides a CompletionStage-based interface for request handling.
     *
     * By default, executes the synchronous handler on the
     * calling thread and wraps its result in a completed future.
     *
     * Implementations may override this method to perform
     * asynchronous processing.
     *
     * @param message the incoming request
     * @return a CompletionStage representing the response
     */
    default CompletionStage<Message> handleRequestAsync(Message message) {

        try {

            return CompletableFuture.completedFuture(
                    handleRequest(message)
            );

        } catch (RuntimeException error) {

            return CompletableFuture.failedFuture(error);
        }
    }
}
