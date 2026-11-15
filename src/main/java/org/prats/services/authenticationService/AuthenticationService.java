package org.prats.services.authenticationService;

import org.prats.definitions.classes.ClientConnection;
import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.Authenticator;
import org.prats.definitions.interfaces.Service;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public final class AuthenticationService
        implements Service, Authenticator {


    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;


    @Override
    public CompletionStage<Message> authenticate(String username, char[] password, ClientConnection connection) {
        return null;
    }

    @Override
    public boolean logout(ClientConnection connection) {
        return false;
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void quiesce() {

    }

    @Override
    public void drain(long timeout) {

    }

    @Override
    public void cleanup() {

    }
}
