
package org.prats.definitions.classes;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.enums.ProcessType;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Standard communication contract used throughout
 * the Mediation Engine.
 *
 * A Message represents a request, response, command,
 * notification or other supported protocol operation.
 *
 * Responsibilities:
 *
 * 1. Identify the individual message.
 * 2. Associate requests and responses through correlation.
 * 3. Describe message type and requested operation.
 * 4. Identify the originating process.
 * 5. Carry structured JSON payloads.
 * 6. Identify the intended recipient.
 * 7. Represent response status and error information.
 * 8. Carry optional authentication session credentials.
 *
 * MessageService owns request/response correlation,
 * while individual services own business processing.
 *
 * Authentication sessions are independent of TCP
 * connections and may remain valid after disconnection.
 *
 * SECURITY:
 *
 * Session identifiers are bearer credentials and must
 * not appear in ordinary application logs.
 *
 * Passwords and other credentials inside payloads
 * must also be excluded from diagnostic output.
 *
 * Local-only fields are excluded from JSON serialization
 * and must not be treated as remotely transferable
 * authorization capabilities.
 */
public final class Message {

    // =========================================================
    // 1. RESPONSE STATE
    // =========================================================

    /**
     * Indicates whether response processing is complete.
     *
     * Defaults to true to preserve existing behavior.
     *
     * MessageService may use this flag for multi-stage
     * request/response delivery.
     */
    private boolean responseComplete = true;

    // =========================================================
    // 2. LOCAL ACCOUNT DATABASE CAPABILITY
    // =========================================================

    /**
     * Local-only marker for authorized account
     * database operations.
     *
     * This field is not serialized to JSON.
     *
     * It is therefore unavailable after normal
     * Message serialization and deserialization.
     *
     * SECURITY:
     *
     * This flag alone must not establish authorization.
     * Trusted service routing and database permissions
     * remain responsible for access control.
     */
    @JsonIgnore
    private boolean accountDatabaseRequest;

    /**
     * Returns the local account database marker.
     *
     * Excluded from JSON serialization.
     */
    @JsonIgnore
    public boolean isAccountDatabaseRequest() {

        return accountDatabaseRequest;
    }

    /**
     * Updates the local account database marker.
     *
     * This operation must only be performed by
     * trusted internal components.
     */
    @JsonIgnore
    public void setAccountDatabaseRequest(
            boolean value
    ) {

        this.accountDatabaseRequest = value;
    }

    // =========================================================
    // 3. LOCAL APPLICATION REQUEST REFERENCE
    // =========================================================

    /**
     * Optional reference to an associated application
     * request within the current JVM.
     *
     * The reference is not transferred over TCP
     * or included in serialized JSON.
     *
     * It may be used by internal processing components
     * to preserve request context during execution.
     */
    @JsonIgnore
    private Message applicationRequest;

    @JsonIgnore
    public Message getApplicationRequest() {

        return applicationRequest;
    }

    @JsonIgnore
    public void setApplicationRequest(
            Message value
    ) {

        this.applicationRequest = value;
    }

    // =========================================================
    // 4. MESSAGE IDENTIFIERS
    // =========================================================

    /**
     * Unique identifier for this individual Message.
     *
     * Each newly constructed Message receives
     * its own random UUID.
     *
     * A request and its response may have different
     * message IDs.
     */
    private final UUID messageId;

    /**
     * Correlates related request and response Messages.
     *
     * A response must preserve the appropriate
     * original request correlation ID.
     *
     * This identifier is not an authentication
     * credential.
     */
    private UUID correlationId;

    // =========================================================
    // 5. AUTHENTICATION SESSION
    // =========================================================

    /**
     * Optional authentication session identifier.
     *
     * Represents an existing authenticated session
     * maintained by the Platform SessionRegistry.
     *
     * This field is independent of:
     *
     * - Message ID.
     * - Correlation ID.
     * - TCP connection identity.
     * - Process identity.
     *
     * Expected usage:
     *
     * LOGIN REQUEST:
     *     No session identifier is required.
     *
     * LOGIN RESPONSE:
     *     The newly issued session identifier may
     *     be returned to the authenticated client.
     *
     * AUTHENTICATED REQUEST:
     *     The client presents its session identifier.
     *
     * LOGOUT REQUEST:
     *     The client presents the session to revoke.
     *
     * INTERNAL MESSAGE:
     *     The field may remain null when another
     *     trusted authentication mechanism applies.
     *
     * SECURITY:
     *
     * A session UUID acts as a bearer credential.
     * Possession must be validated against the
     * authoritative SessionRegistry.
     *
     * This field must never be logged in plaintext.
     *
     * The absence of a session ID does not automatically
     * authorize a request.
     */
    private UUID sessionId;

    // =========================================================
    // 6. MESSAGE CLASSIFICATION
    // =========================================================

    /**
     * Identifies the protocol message category.
     */
    private MessageType messageType;

    /**
     * Identifies the originating process category.
     *
     * Examples may include:
     *
     * PLATFORM
     * EC
     * GUI
     * DB
     *
     * The value is fixed during construction.
     *
     * SECURITY:
     *
     * A process type received from a remote client
     * is not proof of that client's identity.
     */
    private final ProcessType processType;

    /**
     * Name of the process associated with
     * the Message.
     *
     * Local messages initially obtain this
     * value from ProcessContext.
     */
    private String processName;

    /**
     * Requested operation.
     *
     * The operation determines the action requested
     * from the appropriate processing service.
     */
    private Operation operation;

    // =========================================================
    // 7. MESSAGE TIMESTAMP
    // =========================================================

    /**
     * Creation timestamp of the Message.
     *
     * Immutable after construction.
     */
    private final Instant timestamp;

    // =========================================================
    // 8. MESSAGE PAYLOAD
    // =========================================================

    /**
     * Structured JSON payload.
     *
     * The payload contains information specific
     * to the requested service operation.
     *
     * Examples:
     *
     * - Authentication request data.
     * - DatabaseRequest payload.
     * - DatabaseResponse payload.
     * - Container status data.
     * - Health report information.
     *
     * The common Message envelope is independent
     * of individual payload structures.
     */
    private JsonNode payload;

    // =========================================================
    // 9. LOCAL ORIGINAL MESSAGE
    // =========================================================

    /**
     * Optional original serialized message text.
     *
     * Maintained only for local processing.
     *
     * Excluded from JSON serialization.
     *
     * SECURITY:
     *
     * Raw messages may contain credentials,
     * session identifiers and sensitive payloads.
     *
     * Logging this value without redaction
     * can expose authentication information.
     */
    @JsonIgnore
    private String fullStringOriginalMessage;

    // =========================================================
    // 10. MESSAGE DESTINATION
    // =========================================================

    /**
     * Logical destination of the Message.
     *
     * Routing components interpret this field
     * according to the existing message protocol.
     *
     * SECURITY:
     *
     * A recipient supplied by a remote client
     * must be validated before privileged dispatch.
     */
    private String recipientID;

    // =========================================================
    // 11. RESPONSE STATUS
    // =========================================================

    /**
     * Logical result of processing.
     *
     * This is distinct from the transport delivery
     * status maintained by ReplyChannels.
     */
    private String status;

    /**
     * Structured processing error.
     *
     * The existing application Error type
     * is preserved.
     */
    private Error error;

    // =========================================================
    // 12. CONSTRUCTOR - NEW CORRELATION ID
    // =========================================================

    /**
     * Creates a new Message with independently
     * generated message and correlation IDs.
     *
     * The session ID initially remains null.
     *
     * It may be assigned later using setSessionId().
     *
     * @param messageType Message classification.
     * @param processType Originating process category.
     * @param operation Requested operation.
     * @param payload Structured JSON payload.
     * @param fullStringOriginalMessage Original message text.
     * @param recipientID Intended recipient.
     */
    public Message(
            MessageType messageType,
            ProcessType processType,
            Operation operation,
            JsonNode payload,
            String fullStringOriginalMessage,
            String recipientID
    ) {

        this(
                UUID.randomUUID(),
                UUID.randomUUID(),
                messageType,
                processType,
                operation,
                payload,
                fullStringOriginalMessage,
                recipientID,
                Instant.now()
        );
    }

    // =========================================================
    // 13. CONSTRUCTOR - EXISTING CORRELATION ID
    // =========================================================

    /**
     * Creates a Message using an existing
     * correlation identifier.
     *
     * Useful for response Messages that must
     * correlate with an earlier request.
     *
     * The session ID initially remains null.
     *
     * @param messageType Message classification.
     * @param processType Originating process category.
     * @param operation Requested operation.
     * @param payload Structured JSON payload.
     * @param fullStringOriginalMessage Original message text.
     * @param recipientID Intended recipient.
     * @param correlationId Correlation identifier to preserve.
     */
    public Message(
            MessageType messageType,
            ProcessType processType,
            Operation operation,
            JsonNode payload,
            String fullStringOriginalMessage,
            String recipientID,
            UUID correlationId
    ) {

        this(
                UUID.randomUUID(),
                correlationId,
                messageType,
                processType,
                operation,
                payload,
                fullStringOriginalMessage,
                recipientID,
                Instant.now()
        );
    }

    // =========================================================
    // 14. INTERNAL CONSTRUCTOR
    // =========================================================

    /**
     * Shared constructor for new Messages
     * and Jackson-deserialized Messages.
     *
     * Mandatory protocol fields are validated.
     *
     * Optional information may remain null.
     *
     * Session information is assigned independently
     * because authentication is optional at the
     * Message construction level.
     */
    private Message(
            UUID messageId,
            UUID correlationId,
            MessageType messageType,
            ProcessType processType,
            Operation operation,
            JsonNode payload,
            String fullStringOriginalMessage,
            String recipientID,
            Instant timestamp
    ) {

        // -----------------------------------------------------
        // VALIDATE MANDATORY IDENTIFIERS
        // -----------------------------------------------------

        this.messageId =
                Objects.requireNonNull(
                        messageId,
                        "messageId cannot be null"
                );

        this.correlationId =
                Objects.requireNonNull(
                        correlationId,
                        "correlationId cannot be null"
                );

        // -----------------------------------------------------
        // VALIDATE MESSAGE CLASSIFICATION
        // -----------------------------------------------------

        this.messageType =
                Objects.requireNonNull(
                        messageType,
                        "messageType cannot be null"
                );

        this.processType =
                Objects.requireNonNull(
                        processType,
                        "processType cannot be null"
                );

        this.operation =
                Objects.requireNonNull(
                        operation,
                        "operation cannot be null"
                );

        // -----------------------------------------------------
        // VALIDATE MESSAGE TIMESTAMP
        // -----------------------------------------------------

        this.timestamp =
                Objects.requireNonNull(
                        timestamp,
                        "timestamp cannot be null"
                );

        // -----------------------------------------------------
        // INITIALIZE OPTIONAL MESSAGE DATA
        // -----------------------------------------------------

        this.payload = payload;

        this.fullStringOriginalMessage =
                fullStringOriginalMessage;

        this.recipientID =
                recipientID;

        // -----------------------------------------------------
        // INITIALIZE PROCESS CONTEXT
        // -----------------------------------------------------

        this.processName =
                ProcessContext.getProcessName();

        /*
         * A newly constructed Message is not
         * automatically associated with any session.
         *
         * The session identifier may be populated
         * by a trusted caller or by Jackson.
         */
        this.sessionId = null;
    }

    // =========================================================
    // 15. JACKSON CONSTRUCTOR - LEGACY SIGNATURE
    // =========================================================

    /**
     * Preserves the existing constructor signature
     * for code that constructs Messages directly.
     *
     * Jackson uses the extended constructor below
     * to deserialize the optional session identifier.
     */
    public Message(
            UUID messageId,
            UUID correlationId,
            MessageType messageType,
            ProcessType processType,
            ProcessType messageSource,
            Operation operation,
            Instant timestamp
    ) {

        this(
                messageId,
                correlationId,
                messageType,
                processType,
                messageSource,
                operation,
                timestamp,
                null
        );
    }

    // =========================================================
    // 16. JACKSON CONSTRUCTOR - SESSION SUPPORT
    // =========================================================

    /**
     * Jackson property-based constructor.
     *
     * Required protocol fields:
     *
     * - messageId
     * - correlationId
     * - messageType
     * - operation
     * - timestamp
     *
     * Process type may be provided through either
     * processType or legacy messageSource.
     *
     * The optional sessionId property is accepted
     * without changing the mandatory Message fields.
     *
     * Other mutable properties are populated by
     * Jackson through their existing setters.
     *
     * SECURITY:
     *
     * Deserialization does not authenticate a session.
     * SessionRegistry validation must occur separately
     * before any protected operation is dispatched.
     */
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Message(
            @JsonProperty(
                    value = "messageId",
                    required = true
            )
            UUID messageId,

            @JsonProperty(
                    value = "correlationId",
                    required = true
            )
            UUID correlationId,

            @JsonProperty(
                    value = "messageType",
                    required = true
            )
            MessageType messageType,

            @JsonProperty("processType")
            ProcessType processType,

            @JsonProperty("messageSource")
            ProcessType messageSource,

            @JsonProperty(
                    value = "operation",
                    required = true
            )
            Operation operation,

            @JsonProperty(
                    value = "timestamp",
                    required = true
            )
            Instant timestamp,

            @JsonProperty("sessionId")
            UUID sessionId
    ) {

        this(
                messageId,
                correlationId,
                messageType,
                processType != null
                        ? processType
                        : messageSource,
                operation,
                null,
                null,
                null,
                timestamp
        );

        /*
         * Session ID is optional.
         *
         * Login requests and internal Messages
         * may legitimately omit this property.
         */
        this.sessionId = sessionId;
    }

    // =========================================================
    // 17. PROCESS NAME
    // =========================================================

    /**
     * Updates the process name associated
     * with this Message.
     */
    public void setProcessName(
            String processName
    ) {

        this.processName = processName;
    }

    /**
     * Returns the configured process name.
     */
    public String getProcessName() {

        return processName;
    }

    // =========================================================
    // 18. MESSAGE IDENTIFIER
    // =========================================================

    /**
     * Returns the unique identifier
     * of this individual Message.
     */
    public UUID getMessageId() {

        return messageId;
    }

    // =========================================================
    // 19. CORRELATION IDENTIFIER
    // =========================================================

    /**
     * Returns the request/response correlation ID.
     */
    public UUID getCorrelationId() {

        return correlationId;
    }

    /**
     * Assigns the correlation ID used
     * to associate related Messages.
     */
    public void setCorrelationId(
            UUID correlationId
    ) {

        this.correlationId = correlationId;
    }

    /**
     * Generates a new random correlation ID.
     *
     * Useful when initiating an independent
     * internal request.
     *
     * A newly generated correlation ID must
     * be registered with the appropriate
     * ReplyChannel before request dispatch.
     */
    public void generateRandomCorrelationId() {

        this.correlationId =
                UUID.randomUUID();
    }

    // =========================================================
    // 20. SESSION IDENTIFIER
    // =========================================================

    /**
     * Returns the optional authentication
     * session identifier.
     *
     * May return null for unauthenticated
     * or trusted internal Messages.
     *
     * Jackson serializes this property as:
     *
     * "sessionId": "UUID"
     *
     * Null values are omitted from JSON.
     */
    @JsonProperty("sessionId")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public UUID getSessionId() {

        return sessionId;
    }

    /**
     * Assigns an authentication session identifier.
     *
     * This setter does not perform validation.
     *
     * Session validation belongs to the trusted
     * request authorization layer.
     *
     * The identifier must not be logged
     * or disclosed to unrelated recipients.
     */
    @JsonProperty("sessionId")
    public void setSessionId(
            UUID sessionId
    ) {

        this.sessionId = sessionId;
    }

    // =========================================================
    // 21. MESSAGE TYPE
    // =========================================================

    public MessageType getMessageType() {

        return messageType;
    }

    public void setMessageType(
            MessageType messageType
    ) {

        this.messageType = messageType;
    }

    // =========================================================
    // 22. PROCESS TYPE
    // =========================================================

    /**
     * Maintains the existing processType
     * JSON property.
     */
    @JsonProperty("processType")
    public ProcessType getProcessType() {

        return processType;
    }

    // =========================================================
    // 23. OPERATION
    // =========================================================

    public Operation getOperation() {

        return operation;
    }

    public void setOperation(
            Operation operation
    ) {

        this.operation = operation;
    }

    // =========================================================
    // 24. TIMESTAMP
    // =========================================================

    public Instant getTimestamp() {

        return timestamp;
    }

    // =========================================================
    // 25. PAYLOAD
    // =========================================================

    public JsonNode getPayload() {

        return payload;
    }

    public void setPayload(
            JsonNode payload
    ) {

        this.payload = payload;
    }

    // =========================================================
    // 26. ORIGINAL MESSAGE TEXT
    // =========================================================

    /**
     * Returns the original message representation
     * retained within the current JVM.
     *
     * Excluded from JSON serialization.
     */
    @JsonIgnore
    public String getFullStringOriginalMessage() {

        return fullStringOriginalMessage;
    }

    /**
     * Updates the original message representation.
     *
     * This value may contain sensitive information
     * and must not be included in ordinary logs.
     */
    @JsonIgnore
    public void setFullStringOriginalMessage(
            String fullStringOriginalMessage
    ) {

        this.fullStringOriginalMessage =
                fullStringOriginalMessage;
    }

    // =========================================================
    // 27. RECIPIENT
    // =========================================================

    @JsonProperty("recipientID")
    public String getRecipientID() {

        return recipientID;
    }

    @JsonProperty("recipientID")
    public void setRecipientID(
            String recipientID
    ) {

        this.recipientID = recipientID;
    }

    // =========================================================
    // 28. RESPONSE COMPLETION
    // =========================================================

    public boolean isResponseComplete() {

        return responseComplete;
    }

    public void setResponseComplete(
            boolean responseComplete
    ) {

        this.responseComplete = responseComplete;
    }

    // =========================================================
    // 29. RESPONSE STATUS
    // =========================================================

    public String getStatus() {

        return status;
    }

    public void setStatus(
            String status
    ) {

        this.status = status;
    }

    // =========================================================
    // 30. ERROR INFORMATION
    // =========================================================

    public Error getError() {

        return error;
    }

    public void setError(
            Error error
    ) {

        this.error = error;
    }

    /**
     * Returns true when the Message contains
     * structured error information.
     */
    public boolean hasError() {

        return error != null;
    }
}
