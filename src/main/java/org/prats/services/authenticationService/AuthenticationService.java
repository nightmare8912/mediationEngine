
package org.prats.services.authenticationService;

import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.definitions.interfaces.RequestHandler;
import org.prats.services.requestService.RequestExecutionService;

/**
 * Managed authentication execution.
 * All account policy belongs to its handler.
 */
public final class AuthenticationService extends RequestExecutionService {

    public AuthenticationService(
            RequestHandler handler,
            MessageDispatcher dispatcher
    ) {
        super(handler, dispatcher);
    }
}
