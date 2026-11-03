package org.prats.inputHandler.messageForwarders;

import org.prats.definitions.MessageEntity;
import org.prats.services.Service;

public interface MessageForwarder {
    public void forwardMessage(
            MessageEntity messageEntity,
            Service service
    );
}
