package org.prats.forwarders;

import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.Forwarder;
import org.prats.definitions.interfaces.Submittable;
import org.prats.loggers.EngineLogger;

public final class GUIForwarder implements Forwarder {

    private static final EngineLogger log =
            EngineLogger.getLogger(GUIForwarder.class);

    private final Submittable service;

    public GUIForwarder(Submittable service) {

        this.service = service;

        log.debug(() ->
                "[GUIForwarder][constructor] GUI forwarder initialized"
                        + " | service=" + service.getClass().getSimpleName()
        );
    }

    @Override
    public void forward(Message message) {

        log.debug(() ->
                "[GUIForwarder][forward] Incoming GUI message received"
                        + " | correlationId=" + message.getCorrelationId()
                        + " | operation=" + message.getOperation()
                        + " | messageType=" + message.getMessageType()
        );

        log.debug(() ->
                "[GUIForwarder][forward] Forwarding message to target service"
                        + " | correlationId=" + message.getCorrelationId()
                        + " | targetService=" + service.getClass().getSimpleName()
        );

        try {

            service.submit(message);

            log.debug(() ->
                    "[GUIForwarder][forward] Message submitted successfully"
                            + " | correlationId=" + message.getCorrelationId()
                            + " | operation=" + message.getOperation()
                            + " | targetService=" + service.getClass().getSimpleName()
            );

        } catch (RuntimeException exception) {

            log.error(
                    "[GUIForwarder][forward] Failed to forward GUI message"
                            + " | correlationId=" + message.getCorrelationId()
                            + " | operation=" + message.getOperation()
                            + " | targetService=" + service.getClass().getSimpleName()
                            + " | exception=" + exception.getClass().getSimpleName()
                            + " | reason=" + exception.getMessage()
            );

            throw exception;
        }
    }
}