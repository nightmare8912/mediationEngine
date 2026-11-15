package org.prats.definitions.interfaces;

import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;

import java.util.concurrent.CompletionStage;

public interface Authenticator {

    CompletionStage<Message> authenticate(
            String username,
            char[] password,
            ClientConnection connection
    );

    boolean logout(ClientConnection connection);
}
