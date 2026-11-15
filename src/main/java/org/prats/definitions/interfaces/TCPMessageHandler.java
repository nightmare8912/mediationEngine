package org.prats.definitions.interfaces;

import org.prats.definitions.classes.Message;

public interface TCPMessageHandler {

    void onRequest(Message message);

    void onResponse(Message message);

    void onHeartbeat(Message message);

    void onInfo(Message message);

    void onTimeout(Message orignalMessage, Throwable cause);
}
