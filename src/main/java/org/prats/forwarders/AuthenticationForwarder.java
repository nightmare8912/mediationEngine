package org.prats.forwarders;

import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.Forwarder;
import org.prats.definitions.interfaces.Submittable;

/** Routing only; account policy belongs to AuthenticationHandler. */
public final class AuthenticationForwarder implements Forwarder {
    private final Submittable service;
    public AuthenticationForwarder(Submittable service) { this.service = service; }
    @Override public void forward(Message request) { service.submit(request); }
}
