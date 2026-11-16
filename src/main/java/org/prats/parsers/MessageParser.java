
package org.prats.parsers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.prats.definitions.classes.Error;
import org.prats.definitions.classes.Message;

import org.prats.definitions.enums.MessageType;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.enums.ProcessType;

import org.prats.loggers.FileLogger;
import org.prats.loggers.EngineLogger;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Central utility for Message serialization,
 * deserialization, payload conversion and
 * response construction.
 *
 * Responsibilities:
 *
 * 1. Deserialize incoming JSON into Message objects.
 * 2. Serialize Message objects into JSON strings.
 * 3. Convert Java objects into JSON payloads.
 * 4. Deserialize JSON payloads into Java objects.
 * 5. Extract individual fields from message payloads.
 * 6. Add or update fields inside JSON object payloads.
 * 7. Create standard response Messages.
 * 8. Create and attach structured error information.
 *
 * A Message is the standard communication contract
 * between Mediation Engine services.
 *
 * Service-specific request and response records
 * are represented inside the Message payload.
 *
 * The parser does not perform:
 *
 * - Authentication.
 * - Session validation.
 * - Permission validation.
 * - Message routing.
 * - Request/response correlation management.
 *
 * These responsibilities belong to their respective
 * services and processing components.
 *
 * SECURITY:
 *
 * Message payloads may contain authentication
 * credentials, session identifiers and other
 * sensitive information.
 *
 * Raw payloads and serialized Messages must not
 * be included in ordinary diagnostic logs.
 */
public class MessageParser {

    // =========================================================
    // 1. LOGGER
    // =========================================================

    /**
     * Application logger for serialization,
     * deserialization and parser diagnostics.
     */
    private static final EngineLogger log =
            EngineLogger.getLogger(MessageParser.class);

    // =========================================================
    // 2. PARSER DEPENDENCIES
    // =========================================================

    /**
     * Jackson ObjectMapper used for all JSON
     * serialization and deserialization operations.
     *
     * The mapper is initialized once per parser
     * instance and reused across operations.
     */
    private final ObjectMapper objectMapper;

    /**
     * Process category associated with Messages
     * created locally by this parser.
     *
     * Used when constructing response Messages
     * and serialization fallback Messages.
     */
    private final ProcessType processType;

    // =========================================================
    // 3. CONSTRUCTOR
    // =========================================================

    /**
     * Initializes the MessageParser.
     *
     * Configures Jackson to:
     *
     * 1. Support Java time classes through JavaTimeModule.
     * 2. Serialize date/time objects using readable
     *    ISO-8601 representations instead of timestamps.
     *
     * @param processType Process category associated
     *                    with locally generated Messages.
     */
    public MessageParser(
            ProcessType processType
    ) {

        // -----------------------------------------------------
        // INITIALIZE PROCESS IDENTITY
        // -----------------------------------------------------

        this.processType = processType;

        // -----------------------------------------------------
        // INITIALIZE OBJECT MAPPER
        // -----------------------------------------------------

        this.objectMapper = new ObjectMapper();

        // -----------------------------------------------------
        // REGISTER JAVA TIME SUPPORT
        // -----------------------------------------------------

        /*
         * JavaTimeModule enables Jackson to work
         * with modern Java date/time classes.
         *
         * This is required for Message timestamps
         * represented using java.time.Instant.
         */
        objectMapper.registerModule(
                new JavaTimeModule()
        );

        // -----------------------------------------------------
        // CONFIGURE DATE SERIALIZATION
        // -----------------------------------------------------

        /*
         * Disabling timestamp serialization causes
         * supported Java time values to be represented
         * using standard date/time strings.
         */
        objectMapper.disable(
                SerializationFeature.WRITE_DATES_AS_TIMESTAMPS
        );
    }

    // =========================================================
    // 4. DESERIALIZE JSON INTO MESSAGE
    // =========================================================

    /**
     * Attempts to deserialize an incoming string
     * into a standard Message object.
     *
     * Processing sequence:
     *
     * 1. Deserialize the input using ObjectMapper.
     * 2. Preserve the original input string locally.
     * 3. Handle malformed JSON using an UNKNOWN Message.
     * 4. Attach a decoding error when the resulting
     *    MessageType is UNKNOWN.
     *
     * A decoding failure does not propagate directly
     * to the caller.
     *
     * Instead, an UNKNOWN Message is constructed
     * using the original input.
     *
     * The existing command-processing infrastructure
     * can inspect the resulting Message and decide
     * how to handle the input.
     *
     * @param message Incoming serialized message text.
     *
     * @return Parsed Message or an UNKNOWN Message
     *         containing decoding error information.
     */
    public Message createMessageFromJSON(
            String message
    ) {

        Message messageDefinition;

        try {

            // -------------------------------------------------
            // DESERIALIZE INCOMING MESSAGE
            // -------------------------------------------------

            messageDefinition =
                    objectMapper.readValue(
                            message,
                            Message.class
                    );

            // -------------------------------------------------
            // PRESERVE ORIGINAL INPUT
            // -------------------------------------------------

            /*
             * The original incoming representation
             * remains available for local processing.
             *
             * Message excludes this field from
             * JSON serialization.
             */
            messageDefinition.setFullStringOriginalMessage(
                    message
            );

        } catch (JsonProcessingException jsonExe) {

            // -------------------------------------------------
            // HANDLE JSON DECODING FAILURE
            // -------------------------------------------------

            /*
             * Plain-text commands intentionally fall
             * back to the existing command-processing
             * mechanism.
             *
             * Decode failures are recorded using DEBUG
             * diagnostics rather than file error logging
             * for every command request.
             *
             * Only the input length and exception type
             * are included in the diagnostic message.
             */
            log.debug(() ->
                    "[MessageParser][createMessageFromJSON] "
                            + "Input is not a JSON message; "
                            + "routing as command text"
                            + " | messageLength="
                            + (message == null
                            ? -1
                            : message.length())
                            + " | cause="
                            + jsonExe.getClass().getSimpleName()
            );

            // -------------------------------------------------
            // CONSTRUCT FALLBACK MESSAGE
            // -------------------------------------------------

            /*
             * UNKNOWN classifications allow downstream
             * processing components to identify input
             * that was not decoded as a valid Message.
             *
             * The original input is preserved locally.
             */
            messageDefinition = new Message(
                    MessageType.UNKNOWN,
                    ProcessType.UNKNOWN,
                    Operation.UNKNOWN,
                    NullNode.instance,
                    message,
                    null
            );
        }

        // -----------------------------------------------------
        // ATTACH DECODING ERROR WHEN APPLICABLE
        // -----------------------------------------------------

        /*
         * A Message explicitly classified as UNKNOWN
         * receives the same decoding error as a
         * malformed JSON fallback Message.
         */
        if (messageDefinition.getMessageType()
                == MessageType.UNKNOWN) {

            attachError(
                    messageDefinition,
                    "JSON_DECODING_ERROR",
                    "Unable to parse incoming message"
            );
        }

        return messageDefinition;
    }

    // =========================================================
    // 5. SERIALIZE MESSAGE INTO JSON
    // =========================================================

    /**
     * Serializes a Message into its JSON representation.
     *
     * Processing sequence:
     *
     * 1. Attempt normal Message serialization.
     * 2. Log serialization failure when encountered.
     * 3. Construct a fallback UNKNOWN Message.
     * 4. Attach JSON_ENCODING_ERROR to the fallback.
     * 5. Attempt fallback serialization.
     *
     * The existing behavior returns an empty string
     * if both serialization attempts fail.
     *
     * @param message Message to serialize.
     *
     * @return Serialized JSON representation, or
     *         an empty string if both attempts fail.
     */
    public String createJSONFromMessage(
            Message message
    ) {

        String jsonMessage = "";

        try {

            // -------------------------------------------------
            // SERIALIZE ORIGINAL MESSAGE
            // -------------------------------------------------

            jsonMessage =
                    objectMapper.writeValueAsString(
                            message
                    );

        } catch (JsonProcessingException jsonExe) {

            // -------------------------------------------------
            // RECORD SERIALIZATION FAILURE
            // -------------------------------------------------

            FileLogger.log(
                    "[createJSONFromMessage] Error while parsing: "
                            + jsonExe.getMessage()
            );

            try {

                // ---------------------------------------------
                // CONSTRUCT FALLBACK MESSAGE
                // ---------------------------------------------

                /*
                 * The fallback preserves the local
                 * original message reference while
                 * replacing the protocol classification
                 * with UNKNOWN values.
                 *
                 * The configured parser process type
                 * identifies the fallback origin.
                 */
                Message fallbackMessage = new Message(
                        MessageType.UNKNOWN,
                        processType,
                        Operation.UNKNOWN,
                        NullNode.instance,
                        message.getFullStringOriginalMessage(),
                        null
                );

                // ---------------------------------------------
                // ATTACH ENCODING ERROR
                // ---------------------------------------------

                attachError(
                        fallbackMessage,
                        "JSON_ENCODING_ERROR",
                        "Unable to serialize outgoing message"
                );

                // ---------------------------------------------
                // SERIALIZE FALLBACK MESSAGE
                // ---------------------------------------------

                jsonMessage =
                        objectMapper.writeValueAsString(
                                fallbackMessage
                        );

            } catch (JsonProcessingException ignored) {

                // ---------------------------------------------
                // FALLBACK SERIALIZATION FAILED
                // ---------------------------------------------

                /*
                 * The existing behavior records the
                 * fallback failure and preserves the
                 * current jsonMessage value.
                 */
                FileLogger.log(
                        "[createJSONFromMessage] "
                                + "Error while parsing, fallback failed..."
                );
            }
        }

        return jsonMessage;
    }

    // =========================================================
    // 6. EXTRACT INDIVIDUAL PAYLOAD FIELD
    // =========================================================

    /**
     * Extracts a named field from a Message payload
     * and converts it into a requested Java type.
     *
     * Processing sequence:
     *
     * 1. Retrieve the Message payload.
     * 2. Return null when the payload is absent.
     * 3. Retrieve the requested field.
     * 4. Return null when the field is absent.
     * 5. Convert the field using ObjectMapper.
     *
     * The target type may be a standard Java class,
     * enum, record or another Jackson-compatible type.
     *
     * @param message Message containing the payload.
     * @param fieldName JSON field to extract.
     * @param targetType Expected Java type.
     *
     * @param <T> Target Java type.
     *
     * @return Converted field value, or null when
     *         the field is absent or JSON null.
     *
     * @throws JsonProcessingException If Jackson
     *         cannot convert the selected field.
     */
    public <T> T extractPayloadField(
            Message message,
            String fieldName,
            Class<T> targetType
    ) throws JsonProcessingException {

        // -----------------------------------------------------
        // RETRIEVE MESSAGE PAYLOAD
        // -----------------------------------------------------

        JsonNode payload =
                message.getPayload();

        // -----------------------------------------------------
        // CHECK PAYLOAD AVAILABILITY
        // -----------------------------------------------------

        if (payload == null || payload.isNull()) {

            return null;
        }

        // -----------------------------------------------------
        // RETRIEVE REQUESTED FIELD
        // -----------------------------------------------------

        JsonNode fieldValue =
                payload.get(fieldName);

        // -----------------------------------------------------
        // CHECK FIELD AVAILABILITY
        // -----------------------------------------------------

        if (fieldValue == null || fieldValue.isNull()) {

            return null;
        }

        // -----------------------------------------------------
        // CONVERT JSON FIELD INTO TARGET TYPE
        // -----------------------------------------------------

        return objectMapper.treeToValue(
                fieldValue,
                targetType
        );
    }

    // =========================================================
    // 7. CREATE ERROR RESPONSE
    // =========================================================

    /**
     * Creates a standard error response associated
     * with an existing request correlation ID.
     *
     * The generated response contains:
     *
     * - MessageType.RESPONSE
     * - Operation.INFO
     * - The supplied correlation identifier
     * - A null JSON payload
     * - Structured error information
     * - FAILED status
     * - Completed response state
     *
     * @param correlationId Original request correlation ID.
     * @param errorCode Application error identifier.
     * @param errorDescription Human-readable error description.
     *
     * @return Standard error response Message.
     */
    public Message createErrorResponse(
            UUID correlationId,
            String errorCode,
            String errorDescription
    ) {

        // -----------------------------------------------------
        // VALIDATE REQUIRED ARGUMENTS
        // -----------------------------------------------------

        Objects.requireNonNull(
                correlationId,
                "Correlation ID cannot be null"
        );

        Objects.requireNonNull(
                errorCode,
                "Error code cannot be null"
        );

        Objects.requireNonNull(
                errorDescription,
                "Error description cannot be null"
        );

        // -----------------------------------------------------
        // CREATE CORRELATED RESPONSE
        // -----------------------------------------------------

        Message response =
                createResponse(
                        correlationId,
                        NullNode.instance
                );

        // -----------------------------------------------------
        // ATTACH ERROR INFORMATION
        // -----------------------------------------------------

        return attachError(
                response,
                errorCode,
                errorDescription
        );
    }

    // =========================================================
    // 8. ATTACH ERROR TO MESSAGE
    // =========================================================

    /**
     * Attaches structured error information to an
     * existing Message.
     *
     * Processing sequence:
     *
     * 1. Construct an Error object.
     * 2. Set its error code.
     * 3. Set its error description.
     * 4. Associate the Error with the Message.
     * 5. Mark the Message status as FAILED.
     * 6. Mark the response as complete.
     *
     * The supplied Message is modified in place.
     *
     * @param message Message receiving error information.
     * @param code Error identifier.
     * @param description Error description.
     *
     * @return The same Message instance.
     */
    public Message attachError(
            Message message,
            String code,
            String description
    ) {

        // -----------------------------------------------------
        // CREATE ERROR OBJECT
        // -----------------------------------------------------

        Error error =
                new Error();

        // -----------------------------------------------------
        // SET ERROR DETAILS
        // -----------------------------------------------------

        error.setErrorCode(code);

        error.setErrorMessage(description);

        // -----------------------------------------------------
        // ASSOCIATE ERROR WITH MESSAGE
        // -----------------------------------------------------

        message.setError(error);

        // -----------------------------------------------------
        // UPDATE RESPONSE STATE
        // -----------------------------------------------------

        message.setStatus("FAILED");

        message.setResponseComplete(true);

        return message;
    }

    // =========================================================
    // 9. CREATE RESPONSE MESSAGE
    // =========================================================

    /**
     * Creates a standard response Message for
     * an existing correlation identifier.
     *
     * A new message ID is generated automatically
     * by the Message constructor.
     *
     * The original correlation ID is preserved
     * so MessageService can locate the appropriate
     * ReplyChannel.
     *
     * The payload is converted into JsonNode
     * using the parser's ObjectMapper.
     *
     * @param correlationId Correlation ID to preserve.
     * @param payload Response payload object.
     *
     * @return Newly constructed response Message.
     */
    public Message createResponse(
            UUID correlationId,
            Object payload
    ) {

        // -----------------------------------------------------
        // VALIDATE CORRELATION IDENTIFIER
        // -----------------------------------------------------

        Objects.requireNonNull(
                correlationId,
                "Correlation ID cannot be null"
        );

        // -----------------------------------------------------
        // CONSTRUCT CORRELATED RESPONSE
        // -----------------------------------------------------

        return new Message(
                MessageType.RESPONSE,
                processType,
                Operation.INFO,
                toPayload(payload),
                null,
                null,
                correlationId
        );
    }

    // =========================================================
    // 10. ADD OR UPDATE PAYLOAD FIELD
    // =========================================================

    /**
     * Adds or updates a named field inside a Message
     * JSON object payload.
     *
     * Processing sequence:
     *
     * 1. Validate the Message and field name.
     * 2. Retrieve the existing payload.
     * 3. Create an empty ObjectNode when necessary.
     * 4. Deep-copy an existing object payload.
     * 5. Reject non-object payload structures.
     * 6. Convert the supplied value into JsonNode.
     * 7. Insert or replace the requested field.
     * 8. Assign the updated payload to the Message.
     *
     * The Message is modified in place.
     *
     * The existing ObjectNode is copied before
     * modification to avoid directly mutating
     * the previously referenced payload object.
     *
     * @param message Message to update.
     * @param fieldName JSON property name.
     * @param fieldValue Java value to insert.
     *
     * @param <T> Java field value type.
     *
     * @return The same Message instance.
     */
    public <T> Message putPayloadField(
            Message message,
            String fieldName,
            T fieldValue
    ) {

        // -----------------------------------------------------
        // VALIDATE INPUT
        // -----------------------------------------------------

        Objects.requireNonNull(
                message,
                "Message cannot be null"
        );

        Objects.requireNonNull(
                fieldName,
                "Field name cannot be null"
        );

        if (fieldName.isBlank()) {

            throw new IllegalArgumentException(
                    "Field name cannot be blank"
            );
        }

        // -----------------------------------------------------
        // RETRIEVE EXISTING PAYLOAD
        // -----------------------------------------------------

        JsonNode existingPayload =
                message.getPayload();

        ObjectNode payload;

        // -----------------------------------------------------
        // INITIALIZE OR COPY OBJECT PAYLOAD
        // -----------------------------------------------------

        if (existingPayload == null || existingPayload.isNull()) {

            /*
             * Missing payloads are initialized
             * as empty JSON objects.
             */
            payload =
                    objectMapper.createObjectNode();

        } else if (existingPayload.isObject()) {

            /*
             * An existing JSON object is copied
             * before the requested field is updated.
             */
            payload =
                    ((ObjectNode) existingPayload).deepCopy();

        } else {

            /*
             * Non-object payloads cannot accept
             * named JSON properties through this method.
             */
            throw new IllegalArgumentException(
                    "Message payload must be a JSON object"
            );
        }

        // -----------------------------------------------------
        // CONVERT FIELD VALUE
        // -----------------------------------------------------

        /*
         * A null Java value is represented
         * explicitly as JSON null.
         *
         * Other Java values are converted
         * using ObjectMapper.
         */
        JsonNode value = fieldValue == null
                ? NullNode.instance
                : objectMapper.valueToTree(fieldValue);

        // -----------------------------------------------------
        // ADD OR REPLACE FIELD
        // -----------------------------------------------------

        payload.set(
                fieldName,
                value
        );

        // -----------------------------------------------------
        // UPDATE MESSAGE PAYLOAD
        // -----------------------------------------------------

        message.setPayload(payload);

        return message;
    }

    // =========================================================
    // 11. CREATE EMPTY MESSAGE
    // =========================================================

    /**
     * Creates a new Message with:
     *
     * - MessageType.UNKNOWN
     * - The configured local process type
     * - Operation.UNKNOWN
     * - An empty JSON object payload
     * - No original message text
     * - No recipient
     *
     * The Message constructor generates new
     * message and correlation identifiers.
     *
     * This method is useful when constructing
     * a Message incrementally through setters.
     *
     * @return Newly constructed Message.
     */
    public Message createEmptyMessage() {

        return new Message(
                MessageType.UNKNOWN,
                processType,
                Operation.UNKNOWN,
                objectMapper.createObjectNode(),
                null,
                null
        );
    }

    // =========================================================
    // 12. CONVERT JAVA OBJECT INTO JSON NODE
    // =========================================================

    /**
     * Converts a Java object into a JsonNode.
     *
     * Null input is represented as NullNode.instance.
     *
     * Non-null values are converted using the
     * configured ObjectMapper.
     *
     * Conversion failures are recorded through
     * FileLogger and rethrown to the caller.
     *
     * @param value Object to convert.
     *
     * @return Equivalent JsonNode representation.
     */
    public JsonNode convertToJsonNode(
            Object value
    ) {

        // -----------------------------------------------------
        // HANDLE NULL INPUT
        // -----------------------------------------------------

        if (value == null) {

            return NullNode.instance;
        }

        try {

            // -------------------------------------------------
            // CONVERT OBJECT INTO JSON TREE
            // -------------------------------------------------

            return objectMapper.valueToTree(
                    value
            );

        } catch (IllegalArgumentException exception) {

            // -------------------------------------------------
            // RECORD CONVERSION FAILURE
            // -------------------------------------------------

            FileLogger.log(
                    "[convertToJsonNode] Serialization failed: "
                            + exception.getMessage()
            );

            // -------------------------------------------------
            // PROPAGATE ORIGINAL EXCEPTION
            // -------------------------------------------------

            throw exception;
        }
    }

    // =========================================================
    // 13. CONVERT OBJECT INTO MESSAGE PAYLOAD
    // =========================================================

    /**
     * Converts a non-null Java object into
     * a JsonNode suitable for Message.payload.
     *
     * Supported input types depend on Jackson
     * serialization capabilities.
     *
     * Examples:
     *
     * - Java records.
     * - Maps.
     * - Collections.
     * - Primitive wrapper types.
     * - Strings.
     * - Existing JSON-compatible Java objects.
     *
     * This method is intended for converting
     * typed request and response payloads.
     *
     * @param object Object to serialize.
     *
     * @param <T> Input Java type.
     *
     * @return Serialized payload as JsonNode.
     */
    public <T> JsonNode toPayload(
            T object
    ) {

        // -----------------------------------------------------
        // VALIDATE PAYLOAD OBJECT
        // -----------------------------------------------------

        Objects.requireNonNull(
                object,
                "Payload object cannot be null"
        );

        // -----------------------------------------------------
        // CONVERT OBJECT INTO JSON TREE
        // -----------------------------------------------------

        return objectMapper.valueToTree(
                object
        );
    }

    // =========================================================
    // 14. CONVERT JSON PAYLOAD INTO JAVA OBJECT
    // =========================================================

    /**
     * Deserializes a JsonNode payload into
     * a specified Java type.
     *
     * This method supports service-specific
     * request and response payload records.
     *
     * Example:
     *
     * DatabaseRequest databaseRequest =
     *         messageParser.fromPayload(
     *                 message.getPayload(),
     *                 DatabaseRequest.class
     *         );
     *
     * The payload type must be compatible
     * with Jackson deserialization.
     *
     * @param payload JSON payload to deserialize.
     * @param payloadType Expected Java type.
     *
     * @param <T> Target Java type.
     *
     * @return Deserialized Java object.
     *
     * @throws JsonProcessingException If Jackson
     *         cannot deserialize the payload.
     */
    public <T> T fromPayload(
            JsonNode payload,
            Class<T> payloadType
    ) throws JsonProcessingException {

        // -----------------------------------------------------
        // VALIDATE ARGUMENTS
        // -----------------------------------------------------

        Objects.requireNonNull(
                payload,
                "Payload cannot be null"
        );

        Objects.requireNonNull(
                payloadType,
                "Payload type cannot be null"
        );

        // -----------------------------------------------------
        // DESERIALIZE PAYLOAD
        // -----------------------------------------------------

        return objectMapper.treeToValue(
                payload,
                payloadType
        );
    }

    // =========================================================
    // 15. OBJECT MAPPER ACCESS
    // =========================================================

    /**
     * Provides access to the configured ObjectMapper
     * for subclasses of MessageParser.
     *
     * The ObjectMapper is shared by all methods
     * within the current parser instance.
     *
     * @return Configured ObjectMapper.
     */
    protected ObjectMapper getObjectMapper() {

        return objectMapper;
    }
}
