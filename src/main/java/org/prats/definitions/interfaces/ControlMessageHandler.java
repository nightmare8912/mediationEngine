package org.prats.definitions.interfaces;

import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;

public interface ControlMessageHandler {

    void onRequest(
            Message request
    );

    void onInfo(
            Message request
    );

    void onHeartbeat(
            Message response
    );

    void onResponse(
            Message response
    );

    void onTimeout(
            Message originalRequest
    );
}