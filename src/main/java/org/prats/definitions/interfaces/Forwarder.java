package org.prats.definitions.interfaces;

import org.prats.definitions.classes.Message;

public interface Forwarder {
    void forward(Message message);
}
