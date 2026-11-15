package org.prats.forwarders;

import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.Forwarder;
import org.prats.definitions.interfaces.Submittable;
import org.prats.loggers.EngineLogger;

public final class GUIForwarder implements Forwarder {

    private static final EngineLogger log =
            EngineLogger.getLogger(GUIForwarder.class);


    public GUIForwarder() {

    }

    @Override
    public void forward(Message message) {

    }
}