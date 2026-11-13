package org.prats.forwarders;

import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.Forwarder;
import org.prats.definitions.interfaces.Submittable;

public class DatabaseForwarder implements Forwarder {

    private final Submittable submittable;

    public DatabaseForwarder(Submittable submittable) {
        this.submittable = submittable;
    }

    @Override
    public void forward(Message message) {
        submittable.submit(message);
    }
}
