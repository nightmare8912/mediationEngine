package org.prats.handlers;

import org.prats.database.databaseUtilities.DatabaseResourceManager;
import org.prats.database.databaseUtilities.MessageSQLParser;
import org.prats.database.databaseUtilities.sqlHelper.SQLQuery;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.*;
import org.prats.definitions.interfaces.RequestHandler;
import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;
import java.sql.*;
import java.util.*;

/** Prepared JDBC execution with bounded results and correlated terminal responses. */
public class DatabaseRequestHandler implements RequestHandler {
    private static final EngineLogger log = EngineLogger.getLogger(DatabaseRequestHandler.class);
    private final DatabaseResourceManager database;
    private final MessageSQLParser sqlParser = new MessageSQLParser();
    private final MessageParser parser = new MessageParser(ProcessType.DATABASE);
    private static final int MAX_ROWS = 1000;
    private static final int MAX_TEXT = 4096;
    private static final int MAX_RESULT_CHARS = 1_000_000;
    public DatabaseRequestHandler(DatabaseResourceManager database) { this.database = Objects.requireNonNull(database); }

    private PreparedStatement prepare(SQLQuery query, Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(query.query());
        try {
            statement.setQueryTimeout(10); statement.setMaxRows(MAX_ROWS + 1); statement.setMaxFieldSize(MAX_TEXT);
            for (int index = 0; index < query.parameters().size(); index++) statement.setObject(index + 1, query.parameters().get(index));
            return statement;
        } catch (SQLException e) { statement.close(); throw e; }
    }
    public ResultSet executeQuery(SQLQuery query, Connection connection) throws SQLException {
        PreparedStatement statement = prepare(query, connection);
        try { statement.closeOnCompletion(); return statement.executeQuery(); }
        catch (SQLException e) { statement.close(); throw e; }
    }
    public int executeUpdate(SQLQuery query, Connection connection) throws SQLException {
        try (PreparedStatement statement = prepare(query, connection)) { return statement.executeUpdate(); }
    }
    private Object cell(ResultSet rows, int column, int type) throws Exception {
        if (type == Types.BLOB || type == Types.BINARY || type == Types.VARBINARY || type == Types.LONGVARBINARY) return "[binary data]";
        if (type == Types.CLOB || type == Types.NCLOB || type == Types.VARCHAR || type == Types.NVARCHAR
                || type == Types.LONGVARCHAR || type == Types.LONGNVARCHAR || type == Types.CHAR) {
            try (var reader = rows.getCharacterStream(column)) {
                if (reader == null) return null;
                char[] buffer = new char[MAX_TEXT + 1]; int total = 0; int count;
                while (total < buffer.length && (count = reader.read(buffer, total, buffer.length - total)) > 0) total += count;
                return new String(buffer, 0, Math.min(total, MAX_TEXT)) + (total > MAX_TEXT ? "…" : "");
            }
        }
        Object value = rows.getObject(column);
        return value == null || value instanceof Number || value instanceof Boolean ? value : value.toString();
    }
    public Message parseDatabaseQueryResponseToMessage(Message request, ResultSet rows) {
        try {
            ResultSetMetaData metadata = rows.getMetaData();
            List<String> columns = new ArrayList<>();
            for (int index = 1; index <= metadata.getColumnCount(); index++) columns.add(metadata.getColumnLabel(index));
            List<Map<String, Object>> result = new ArrayList<>(); int size = 0; boolean truncated = false;
            while (rows.next()) {
                if (result.size() >= MAX_ROWS || size >= MAX_RESULT_CHARS) { truncated = true; break; }
                Map<String, Object> row = new LinkedHashMap<>();
                for (int index = 1; index <= metadata.getColumnCount(); index++) {
                    Object value = cell(rows, index, metadata.getColumnType(index));
                    row.put(columns.get(index - 1), value); size += value == null ? 4 : value.toString().length();
                }
                result.add(row);
            }
            return response(request, Map.of("ok", true, "columns", columns, "rows", result,
                    "rowCount", result.size(), "truncated", truncated, "body", "Query completed."));
        } catch (Exception e) { return error(request, "RESULT_CONVERSION_FAILED", "Unable to convert database results."); }
    }
    public Message parseDatabaseUpdateResponseToMessageEntity(Message request, int count) {
        return response(request, Map.of("ok", true, "affectedRows", count, "body", "Update completed. Rows affected: " + count));
    }
    private Message response(Message request, Map<String, ?> values) {
        Message response = parser.createResponse(request.getCorrelationId(), Map.of());
        response.setPayload(parser.convertToJsonNode(values)); response.setResponseComplete(true);
        response.setStatus(Boolean.TRUE.equals(values.get("ok")) ? "SUCCESS" : "FAILED");
        return response;
    }
    public Message error(Message request, String code, String reason) {
        log.warn(() -> "[DatabaseRequestHandler][error] Database request rejected or failed | code="
                + code + " | correlationId=" + request.getCorrelationId());
        return response(request, Map.of("ok", false, "errorCode", code, "errorDescription", reason, "body", reason));
    }
    @Override public Message handleRequest(Message request) {
        log.debug(() -> "[DatabaseRequestHandler][handleRequest] Executing database operation | operation="
                + request.getOperation() + " | correlationId=" + request.getCorrelationId());
        try {
            if (!Set.of(Operation.FETCH, Operation.SEARCH, Operation.CREATE, Operation.UPDATE, Operation.DELETE).contains(request.getOperation()))
                return error(request, "UNSUPPORTED_OPERATION", "Unsupported database operation.");
            String entity = request.getPayload() == null ? "" : request.getPayload().path("entity").asText("");
            if (entity.equalsIgnoreCase("users")) return error(request, "PROTECTED_ENTITY", "Use account commands to manage users.");
            SQLQuery query = sqlParser.createSQLFromMessage(request);
            if (query == null || query.query() == null || query.query().isBlank()) return error(request, "INVALID_REQUEST", "Invalid database request.");
            try (Connection connection = database.getDataSource().getConnection()) {
                if (request.getOperation() == Operation.FETCH || request.getOperation() == Operation.SEARCH) {
                    try (ResultSet rows = executeQuery(query, connection)) { return parseDatabaseQueryResponseToMessage(request, rows); }
                }
                return parseDatabaseUpdateResponseToMessageEntity(request, executeUpdate(query, connection));
            }
        } catch (IllegalArgumentException e) { return error(request, "INVALID_REQUEST", "Invalid database request."); }
        catch (SQLException e) {
            log.error("[DatabaseRequestHandler][handleRequest] Database operation failed | sqlState="
                    + e.getSQLState() + " | correlationId=" + request.getCorrelationId());
            return error(request, "DATABASE_ERROR", "Database operation failed. SQL state: " + e.getSQLState());
        } catch (RuntimeException e) {
            log.error("[DatabaseRequestHandler][handleRequest] Database unavailable | correlationId=" + request.getCorrelationId());
            return error(request, "DATABASE_UNAVAILABLE", "Database service is unavailable.");
        }
    }
}
