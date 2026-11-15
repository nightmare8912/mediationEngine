package org.prats.forwarders;

import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.Forwarder;
import org.prats.definitions.interfaces.Submittable;
import org.prats.loggers.EngineLogger;

public class TCPForwarder implements Forwarder {

    private static final EngineLogger log =
            EngineLogger.getLogger(TCPForwarder.class);

    private final Submittable submittable;

    public TCPForwarder(Submittable submittable) {
        this.submittable = submittable;

        log.debug(
                "[TCPForwarder][constructor] TCP forwarder initialized"
        );
    }

    @Override
    public void forward(Message message) {

        log.debug(() ->
                "[TCPForwarder][forward] Submitting TCP message"
                        + " | messagePresent=" + (message != null)
        );

        submittable.submit(message);

        log.debug(() ->
                "[TCPForwarder][forward] TCP message submitted successfully"
                        + " | messagePresent=" + (message != null)
        );
    }
}