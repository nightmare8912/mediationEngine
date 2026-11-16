
package org.prats.definitions.interfaces;

import org.prats.definitions.classes.Message;

/**
 * Defines the common authentication service contract.
 *
 * All authentication operations use the standard
 * Mediation Engine Message protocol.
 *
 * Supported authentication workflows may include:
 *
 * 1. User authentication.
 * 2. Account creation.
 * 3. Session refresh.
 * 4. Session logout.
 * 5. Password management.
 *
 * The authentication service is responsible for:
 *
 * - Request admission.
 * - Worker execution.
 * - Service lifecycle management.
 * - Delegation to AuthenticationHandler.
 *
 * AuthenticationHandler is responsible for:
 *
 * - Interpreting the request operation and payload.
 * - Executing the authentication workflow.
 * - Creating the appropriate response Message.
 * - Dispatching responses through MessageService.
 *
 * Response delivery is asynchronous and managed
 * by MessageService using its correlation registry
 * and registered ReplyChannel.
 *
 * No CompletionStage is returned because response
 * correlation and delivery are handled separately.
 *
 * Sessions are independent of TCP connections.
 * Session credentials are validated against the
 * authoritative InMemorySessionManager.
 */
public interface Authenticator {

    /**
     * Accepts an authentication-related Message
     * for asynchronous processing.
     *
     * The implementation must validate service
     * availability and enforce admission limits
     * before submitting the request for execution.
     *
     * AuthenticationHandler determines the requested
     * operation and dispatches the resulting response
     * through MessageService.
     *
     * Rejected requests must result in a structured
     * error response when a valid reply context exists.
     *
     * @param request Incoming authentication Message.
     */
    void processRequest(Message request);
}
