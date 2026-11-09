package org.prats.handlers;

import org.prats.definitions.Message;

public interface RequestHandler {
    Message handleRequest(Message message);
}
