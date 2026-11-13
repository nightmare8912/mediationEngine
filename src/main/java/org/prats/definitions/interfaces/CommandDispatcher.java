package org.prats.definitions.interfaces;

import org.prats.definitions.classes.Message;

public interface CommandDispatcher {
    void dispatchCommand(Message command);
}
