package org.prats.definitions.interfaces;

import org.prats.definitions.classes.Message;

public interface RequestHandler {
    Message handleRequest(Message message);
}
