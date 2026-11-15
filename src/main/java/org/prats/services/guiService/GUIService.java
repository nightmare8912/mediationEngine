package org.prats.services.guiService;

import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.definitions.interfaces.RequestHandler;
import org.prats.services.requestService.RequestExecutionService;

/** Managed GUI request execution. GUI policy and formatting belong to GUIHandler. */
public final class GUIService extends RequestExecutionService {
    public GUIService(RequestHandler handler, MessageDispatcher dispatcher) { super(handler, dispatcher); }
}
