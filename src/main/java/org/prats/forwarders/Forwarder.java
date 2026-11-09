package org.prats.forwarders;

import org.prats.definitions.Message;

public interface Forwarder {
    void forward(Message message);
}
