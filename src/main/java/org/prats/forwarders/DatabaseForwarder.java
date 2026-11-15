package org.prats.forwarders;

import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.Forwarder;
import org.prats.definitions.interfaces.Submittable;
import org.prats.loggers.EngineLogger;

public class DatabaseForwarder implements Forwarder {

    private static final EngineLogger log =
            EngineLogger.getLogger(DatabaseForwarder.class);

    private final Submittable submittable;

    public DatabaseForwarder(Submittable submittable) {
        this.submittable = submittable;

        log.debug(
                "[DatabaseForwarder][constructor] Database forwarder initialized"
        );
    }

    @Override
    public void forward(Message message) {

        log.debug(() ->
                "[DatabaseForwarder][forward] Submitting database request"
                        + " | messagePresent=" + (message != null)
        );

        submittable.submit(message);

        log.debug(() ->
                "[DatabaseForwarder][forward] Database request submitted successfully"
                        + " | messagePresent=" + (message != null)
        );
    }
}