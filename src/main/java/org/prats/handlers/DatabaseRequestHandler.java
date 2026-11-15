
package org.prats.handlers;

import org.prats.database.databaseUtilities.DatabaseResourceManager;
import org.prats.database.databaseUtilities.MessageSQLParser;
import org.prats.database.databaseUtilities.sqlHelper.SQLQuery;

import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.enums.ProcessType;
import org.prats.definitions.interfaces.RequestHandler;

import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Prepared JDBC execution with bounded results
 * and correlated terminal responses.
 */
public class DatabaseRequestHandler implements RequestHandler {

    private static final EngineLogger log =
            EngineLogger.getLogger(DatabaseRequestHandler.class);

    private static final int MAX_ROWS = 1000;
    private static final int MAX_TEXT = 4096;
    private static final int MAX_RESULT_CHARS = 1_000_000;

    private final DatabaseResourceManager database;

    private final MessageSQLParser sqlParser =
            new MessageSQLParser();

    private final MessageParser parser =
            new MessageParser(ProcessType.DATABASE);

    private volatile boolean accountSchemaReady;


    // =========================================================
    // 1. CONSTRUCTOR
    // =========================================================

    public DatabaseRequestHandler(
            DatabaseResourceManager database
    ) {

        this.database = Objects.requireNonNull(database);

        log.debug(() ->
                "[DatabaseRequestHandler][constructor] Database request handler initialized"
        );
    }


    // =========================================================
    // 2. PREPARED STATEMENT CREATION
    // =========================================================

    private PreparedStatement prepare(
            SQLQuery query,
            Connection connection
    ) throws SQLException {

        PreparedStatement statement =
                connection.prepareStatement(query.query());

        try {

            statement.setQueryTimeout(10);

            statement.setMaxRows(MAX_ROWS + 1);

            statement.setMaxFieldSize(MAX_TEXT);

            for (int index = 0;
                 index < query.parameters().size();
                 index++) {

                statement.setObject(
                        index + 1,
                        query.parameters().get(index)
                );
            }

            return statement;

        } catch (SQLException exception) {

            statement.close();

            throw exception;
        }
    }


    // =========================================================
    // 3. DATABASE QUERY EXECUTION
    // =========================================================

    public ResultSet executeQuery(
            SQLQuery query,
            Connection connection
    ) throws SQLException {

        PreparedStatement statement =
                prepare(query, connection);

        try {

            statement.closeOnCompletion();

            return statement.executeQuery();

        } catch (SQLException exception) {

            statement.close();

            throw exception;
        }
    }


    // =========================================================
    // 4. DATABASE UPDATE EXECUTION
    // =========================================================

    public int executeUpdate(
            SQLQuery query,
            Connection connection
    ) throws SQLException {

        try (PreparedStatement statement =
                     prepare(query, connection)) {

            return statement.executeUpdate();
        }
    }


    // =========================================================
    // 5. RESULT SET CELL CONVERSION
    // =========================================================

    private Object cell(
            ResultSet rows,
            int column,
            int type
    ) throws Exception {


        // -----------------------------------------------------
        // 5.1. Handle binary data
        // -----------------------------------------------------

        if (type == Types.BLOB
                || type == Types.BINARY
                || type == Types.VARBINARY
                || type == Types.LONGVARBINARY) {

            return "[binary data]";
        }


        // -----------------------------------------------------
        // 5.2. Handle character data
        // -----------------------------------------------------

        if (type == Types.CLOB
                || type == Types.NCLOB
                || type == Types.VARCHAR
                || type == Types.NVARCHAR
                || type == Types.LONGVARCHAR
                || type == Types.LONGNVARCHAR
                || type == Types.CHAR) {

            try (var reader = rows.getCharacterStream(column)) {

                if (reader == null) {
                    return null;
                }

                char[] buffer = new char[MAX_TEXT + 1];

                int total = 0;
                int count;

                while (total < buffer.length
                        && (count = reader.read(
                        buffer,
                        total,
                        buffer.length - total
                )) > 0) {

                    total += count;
                }

                return new String(
                        buffer,
                        0,
                        Math.min(total, MAX_TEXT)
                ) + (total > MAX_TEXT ? "…" : "");
            }
        }


        // -----------------------------------------------------
        // 5.3. Handle remaining SQL data types
        // -----------------------------------------------------

        Object value = rows.getObject(column);

        return value == null
                || value instanceof Number
                || value instanceof Boolean

                ? value
                : value.toString();
    }


    // =========================================================
    // 6. DATABASE QUERY RESPONSE CONVERSION
    // =========================================================

    public Message parseDatabaseQueryResponseToMessage(
            Message request,
            ResultSet rows
    ) {

        try {

            ResultSetMetaData metadata =
                    rows.getMetaData();


            // -------------------------------------------------
            // 6.1. Extract column metadata
            // -------------------------------------------------

            List<String> columns = new ArrayList<>();

            for (int index = 1;
                 index <= metadata.getColumnCount();
                 index++) {

                columns.add(
                        metadata.getColumnLabel(index)
                );
            }


            // -------------------------------------------------
            // 6.2. Initialize result collection
            // -------------------------------------------------

            List<Map<String, Object>> result =
                    new ArrayList<>();

            int size = 0;

            boolean truncated = false;


            // -------------------------------------------------
            // 6.3. Iterate through result rows
            // -------------------------------------------------

            while (rows.next()) {

                if (result.size() >= MAX_ROWS
                        || size >= MAX_RESULT_CHARS) {

                    truncated = true;

                    break;
                }

                Map<String, Object> row =
                        new LinkedHashMap<>();


                // ---------------------------------------------
                // Convert each column value
                // ---------------------------------------------

                for (int index = 1;
                     index <= metadata.getColumnCount();
                     index++) {

                    Object value = cell(
                            rows,
                            index,
                            metadata.getColumnType(index)
                    );

                    row.put(
                            columns.get(index - 1),
                            value
                    );

                    size += value == null
                            ? 4
                            : value.toString().length();
                }

                result.add(row);
            }


            // -------------------------------------------------
            // 6.4. Construct successful response
            // -------------------------------------------------

            return response(
                    request,
                    Map.of(
                            "ok", true,
                            "columns", columns,
                            "rows", result,
                            "rowCount", result.size(),
                            "truncated", truncated,
                            "body", "Query completed."
                    )
            );

        } catch (Exception exception) {

            log.error(
                    "[DatabaseRequestHandler][parseDatabaseQueryResponseToMessage] Result conversion failed"
                            + " | correlationId=" + request.getCorrelationId(),
                    exception
            );

            return error(
                    request,
                    "RESULT_CONVERSION_FAILED",
                    "Unable to convert database results."
            );
        }
    }


    // =========================================================
    // 7. DATABASE UPDATE RESPONSE CONVERSION
    // =========================================================

    public Message parseDatabaseUpdateResponseToMessageEntity(
            Message request,
            int count
    ) {

        return response(
                request,
                Map.of(
                        "ok", true,
                        "affectedRows", count,
                        "body", "Update completed. Rows affected: " + count
                )
        );
    }


    // =========================================================
    // 8. DATABASE RESPONSE CREATION
    // =========================================================

    private Message response(
            Message request,
            Map<String, ?> values
    ) {

        Message response = parser.createResponse(
                request.getCorrelationId(),
                Map.of()
        );

        response.setPayload(
                parser.convertToJsonNode(values)
        );

        response.setResponseComplete(true);

        response.setStatus(
                Boolean.TRUE.equals(values.get("ok"))
                        ? "SUCCESS"
                        : "FAILED"
        );

        return response;
    }


    // =========================================================
    // 9. DATABASE ERROR RESPONSE
    // =========================================================

    public Message error(
            Message request,
            String code,
            String reason
    ) {

        log.warn(() ->
                "[DatabaseRequestHandler][error] Database request rejected or failed"
                        + " | code=" + code
                        + " | correlationId=" + request.getCorrelationId()
        );

        return response(
                request,
                Map.of(
                        "ok", false,
                        "errorCode", code,
                        "errorDescription", reason,
                        "body", reason
                )
        );
    }


    // =========================================================
    // 10. ACCOUNT DATABASE SCHEMA INITIALIZATION
    // =========================================================

    private synchronized void initializeAccountSchema()
            throws SQLException {

        if (accountSchemaReady) {
            return;
        }

        log.debug(() ->
                "[DatabaseRequestHandler][initializeAccountSchema] Initializing account database schema"
        );

        try (
                Connection connection =
                        database.getDataSource().getConnection();

                Statement statement =
                        connection.createStatement()
        ) {

            statement.execute(
                    "CREATE TABLE IF NOT EXISTS users ("
                            + "user_name VARCHAR(30) PRIMARY KEY, "
                            + "password VARCHAR(255) NOT NULL, "
                            + "active VARCHAR(1) NOT NULL"
                            + ")"
            );

            statement.execute(
                    "ALTER TABLE users ALTER COLUMN password VARCHAR(255)"
            );
        }

        accountSchemaReady = true;

        log.info(
                "[DatabaseRequestHandler][initializeAccountSchema] Account database schema initialized successfully"
        );
    }


    // =========================================================
    // 11. DATABASE REQUEST HANDLING
    // =========================================================

    @Override
    public Message handleRequest(Message request) {

        log.debug(() ->
                "[DatabaseRequestHandler][handleRequest] Executing database operation"
                        + " | operation=" + request.getOperation()
                        + " | correlationId=" + request.getCorrelationId()
        );

        try {


            // -------------------------------------------------
            // 11.1. Validate supported database operation
            // -------------------------------------------------

            if (!Set.of(
                    Operation.FETCH,
                    Operation.SEARCH,
                    Operation.CREATE,
                    Operation.UPDATE,
                    Operation.DELETE
            ).contains(request.getOperation())) {

                return error(
                        request,
                        "UNSUPPORTED_OPERATION",
                        "Unsupported database operation."
                );
            }


            // -------------------------------------------------
            // 11.2. Resolve target entity
            // -------------------------------------------------

            String entity = request.getPayload() == null
                    ? ""
                    : request.getPayload()
                    .path("entity")
                    .asText("");


            // -------------------------------------------------
            // 11.3. Protect account database operations
            // -------------------------------------------------

            if (entity.equalsIgnoreCase("users")
                    && !request.isAccountDatabaseRequest()) {

                return error(
                        request,
                        "PROTECTED_ENTITY",
                        "Use account commands to manage users."
                );
            }


            // -------------------------------------------------
            // 11.4. Initialize account schema if required
            // -------------------------------------------------

            if (entity.equalsIgnoreCase("users")) {

                initializeAccountSchema();
            }


            // -------------------------------------------------
            // 11.5. Parse structured database request
            // -------------------------------------------------

            SQLQuery query =
                    sqlParser.createSQLFromMessage(request);

            if (query == null
                    || query.query() == null
                    || query.query().isBlank()) {

                return error(
                        request,
                        "INVALID_REQUEST",
                        "Invalid database request."
                );
            }


            // -------------------------------------------------
            // 11.6. Acquire database connection
            // -------------------------------------------------

            try (Connection connection =
                         database.getDataSource().getConnection()) {


                // ---------------------------------------------
                // Execute read operations
                // ---------------------------------------------

                if (request.getOperation() == Operation.FETCH
                        || request.getOperation() == Operation.SEARCH) {

                    try (ResultSet rows =
                                 executeQuery(query, connection)) {

                        return parseDatabaseQueryResponseToMessage(
                                request,
                                rows
                        );
                    }
                }


                // ---------------------------------------------
                // Execute write operations
                // ---------------------------------------------

                return parseDatabaseUpdateResponseToMessageEntity(
                        request,
                        executeUpdate(query, connection)
                );
            }

        } catch (IllegalArgumentException exception) {

            log.warn(() ->
                    "[DatabaseRequestHandler][handleRequest] Invalid database request"
                            + " | correlationId=" + request.getCorrelationId()
            );

            return error(
                    request,
                    "INVALID_REQUEST",
                    "Invalid database request."
            );

        } catch (SQLException exception) {

            log.error(
                    "[DatabaseRequestHandler][handleRequest] Database operation failed"
                            + " | sqlState=" + exception.getSQLState()
                            + " | correlationId=" + request.getCorrelationId()
            );

            return error(
                    request,

                    "23505".equals(exception.getSQLState())
                            ? "DUPLICATE_KEY"
                            : "DATABASE_ERROR",

                    "Database operation failed. SQL state: "
                            + exception.getSQLState()
            );

        } catch (RuntimeException exception) {

            log.error(
                    "[DatabaseRequestHandler][handleRequest] Database unavailable"
                            + " | correlationId=" + request.getCorrelationId()
            );

            return error(
                    request,
                    "DATABASE_UNAVAILABLE",
                    "Database service is unavailable."
            );
        }
    }
}
