package org.prats.definitions.interfaces;

import org.prats.definitions.classes.Message;
import java.util.concurrent.CompletionStage;

/** A handler whose subrequests complete through reply channels, without waiting on workers. */
public interface AsyncRequestHandler extends RequestHandler {
    @Override CompletionStage<Message> handleRequestAsync(Message request);

    @Override default Message handleRequest(Message request) {
        throw new UnsupportedOperationException("Use handleRequestAsync for this handler");
    }
}
