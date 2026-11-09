package org.prats.forwarders;

import org.prats.definitions.Message;
import org.prats.services.databaseService.Submittable;

public class TCPForwarder implements Forwarder {

    private final Submittable submittable;

    public TCPForwarder(Submittable submittable) {
        this.submittable = submittable;
    }

    @Override
    public void forward(Message message) {
        submittable.submit(message);
    }
}
