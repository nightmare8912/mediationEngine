package org.prats.services;

import org.prats.definitions.Message;

public interface CommandDispatcher {
    void dispatchCommand(Message command);
}
