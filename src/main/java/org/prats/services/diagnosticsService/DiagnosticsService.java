package org.prats.services.diagnosticsService;

import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.definitions.interfaces.RequestHandler;
import org.prats.services.requestService.RequestExecutionService;

public final class DiagnosticsService extends RequestExecutionService {
    public DiagnosticsService(RequestHandler handler, MessageDispatcher dispatcher) { super(handler, dispatcher); }
}
