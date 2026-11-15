
package org.prats.database.databaseUtilities;

import com.fasterxml.jackson.databind.JsonNode;

import org.prats.database.databaseUtilities.sqlHelper.SQLQuery;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.Operation;
import org.prats.loggers.EngineLogger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Converts structured JSON database requests into
 * parameterized SQL statements.
 *
 * Supported operations:
 *
 * FETCH  -> SELECT without filters
 * SEARCH -> SELECT with optional filters
 * CREATE -> INSERT
 * UPDATE -> UPDATE with mandatory filters
 * DELETE -> DELETE with mandatory filters
 *
 * The parser returns SQLQuery containing:
 *
 * 1. SQL statement with '?' placeholders.
 * 2. Parameter values in placeholder order.
 *
 * Example:
 *
 * SELECT ID, NAME FROM AGENT WHERE STATUS = ?
 *
 * Parameters:
 *
 * ["ACTIVE"]
 *
 * IMPORTANT:
 *
 * This class builds SQL. It does not execute SQL,
 * authenticate users, or authorize individual requests.
 *
 * Authorization must be enforced before a request
 * reaches the database execution layer.
 *
 * This parser is stateless per request and can be shared
 * between concurrent threads.
 */
public class MessageSQLParser {

    private static final EngineLogger log =
            EngineLogger.getLogger(MessageSQLParser.class);

    // =========================================================
    // 1. VALIDATION CONSTANTS
    // =========================================================

    /*
     * SQL identifiers may contain letters, numbers
     * and underscores, but cannot begin with a number.
     *
     * This blocks SQL syntax from being inserted
     * directly into table and column names.
     *
     * It is NOT a replacement for an allowlist.
     */
    private static final Pattern SQL_IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /*
     * Limits protect against accidentally enormous
     * requests and maliciously complex input.
     */
    private static final int MAX_IDENTIFIER_LENGTH = 128;

    private static final int MAX_SELECTED_COLUMNS = 100;

    private static final int MAX_DATA_FIELDS = 100;

    private static final int MAX_FILTER_DEPTH = 12;

    private static final int MAX_FILTER_NODES = 100;

    private static final int MAX_PARAMETERS = 200;

    private static final int MAX_TEXT_LENGTH = 4096;

    private static final int MAX_SQL_LENGTH = 16_384;

    // =========================================================
    // 2. TABLE ACCESS POLICY
    // =========================================================

    /**
     * Defines which database operations and columns
     * a parser instance is permitted to generate SQL for.
     *
     * readableColumns:
     *     Columns permitted in SELECT and WHERE clauses.
     *
     * writableColumns:
     *     Columns permitted in INSERT and UPDATE data.
     *
     * allowedOperations:
     *     Permitted database operations for this table.
     *
     * This is a SQL-generation policy, not a user-role
     * authorization mechanism.
     */
    public record TablePolicy(
            Set<String> readableColumns,
            Set<String> writableColumns,
            Set<Operation> allowedOperations
    ) {

        public TablePolicy {

            readableColumns = normalizeAllowedColumns(
                    readableColumns
            );

            writableColumns = normalizeAllowedColumns(
                    writableColumns
            );

            allowedOperations = Set.copyOf(
                    Objects.requireNonNull(
                            allowedOperations,
                            "Allowed operations cannot be null"
                    )
            );

            if (allowedOperations.isEmpty()) {
                throw new IllegalArgumentException(
                        "Table policy must allow at least one operation"
                );
            }

            for (Operation operation : allowedOperations) {

                if (operation != Operation.FETCH
                        && operation != Operation.SEARCH
                        && operation != Operation.CREATE
                        && operation != Operation.UPDATE
                        && operation != Operation.DELETE) {

                    throw new IllegalArgumentException(
                            "Unsupported table policy operation: "
                                    + operation
                    );
                }
            }
        }
    }

    /*
     * The allowlist is immutable after initialization.
     *
     * A configured parser rejects tables that are not
     * explicitly included in this map.
     */
    private final Map<String, TablePolicy> tablePolicies;

    private final boolean enforceAllowlist;

    // =========================================================
    // 3. CONSTRUCTORS
    // =========================================================

    /**
     * Legacy constructor.
     *
     * Preserves compatibility with the existing
     * DatabaseService initialization.
     *
     * Identifier syntax and complexity restrictions
     * are enforced, but there is NO table allowlist.
     *
     * Do not expose this unrestricted configuration
     * to untrusted GUI or external client requests.
     */
    public MessageSQLParser() {

        this.tablePolicies = Map.of();
        this.enforceAllowlist = false;

        log.warn(
                "[MessageSQLParser][constructor] "
                        + "Parser initialized without a table allowlist"
                        + " | mode=LEGACY"
                        + " | database authorization must be enforced externally"
        );
    }

    /**
     * Restricted constructor.
     *
     * Table names, column names and operations
     * must match the supplied allowlist.
     *
     * An empty map denies access to every table.
     */
    public MessageSQLParser(
            Map<String, TablePolicy> policies
    ) {

        Objects.requireNonNull(
                policies,
                "Table policies cannot be null"
        );

        Map<String, TablePolicy> normalizedPolicies =
                new HashMap<>();

        for (Map.Entry<String, TablePolicy> entry :
                policies.entrySet()) {

            String tableName = requireIdentifier(
                    entry.getKey()
            ).toUpperCase(Locale.ROOT);

            TablePolicy policy = Objects.requireNonNull(
                    entry.getValue(),
                    "Table policy cannot be null"
            );

            if (normalizedPolicies.putIfAbsent(
                    tableName,
                    policy
            ) != null) {

                throw new IllegalArgumentException(
                        "Duplicate table policy: " + tableName
                );
            }
        }

        this.tablePolicies =
                Map.copyOf(normalizedPolicies);

        this.enforceAllowlist = true;

        log.info(() ->
                "[MessageSQLParser][constructor] "
                        + "Restricted SQL parser initialized"
                        + " | allowedTables="
                        + tablePolicies.size()
        );
    }

    // =========================================================
    // 4. IDENTIFIER VALIDATION
    // =========================================================

    /**
     * Ensures an identifier contains only supported
     * SQL identifier characters.
     *
     * This method does not establish whether the
     * identifier is authorized.
     */
    private static String requireIdentifier(
            String identifier
    ) {

        if (identifier == null
                || identifier.isBlank()
                || identifier.length() > MAX_IDENTIFIER_LENGTH
                || !SQL_IDENTIFIER.matcher(identifier).matches()) {

            throw new IllegalArgumentException(
                    "Invalid SQL identifier"
            );
        }

        return identifier;
    }

    /**
     * Normalizes configured column names.
     *
     * Locale.ROOT avoids locale-dependent casing.
     */
    private static Set<String> normalizeAllowedColumns(
            Set<String> columns
    ) {

        Objects.requireNonNull(
                columns,
                "Allowed columns cannot be null"
        );

        Set<String> normalized = new HashSet<>();

        for (String column : columns) {

            normalized.add(
                    requireIdentifier(column)
                            .toUpperCase(Locale.ROOT)
            );
        }

        return Set.copyOf(normalized);
    }

    // =========================================================
    // 5. TABLE AND COLUMN ACCESS VALIDATION
    // =========================================================

    /**
     * Validates the table name and checks the
     * configured operation allowlist.
     */
    private String requireEntity(
            JsonNode payload,
            Operation operation
    ) {

        JsonNode entityNode = payload.path("entity");

        if (!entityNode.isTextual()) {

            throw new IllegalArgumentException(
                    "Database entity must be a string"
            );
        }

        String table = requireIdentifier(
                entityNode.textValue()
        );

        if (!enforceAllowlist) {
            return table;
        }

        TablePolicy policy = tablePolicies.get(
                table.toUpperCase(Locale.ROOT)
        );

        if (policy == null) {

            log.warn(() ->
                    "[MessageSQLParser][requireEntity] "
                            + "Database table rejected by allowlist"
            );

            throw new IllegalArgumentException(
                    "Database table is not permitted"
            );
        }

        if (!policy.allowedOperations().contains(operation)) {

            log.warn(() ->
                    "[MessageSQLParser][requireEntity] "
                            + "Database operation rejected by table policy"
                            + " | operation=" + operation
            );

            throw new IllegalArgumentException(
                    "Database operation is not permitted"
            );
        }

        return table;
    }

    /**
     * Validates whether a column can be read or written.
     *
     * Read access is used for:
     *
     * SELECT columns
     * WHERE conditions
     *
     * Write access is used for:
     *
     * INSERT data
     * UPDATE assignments
     */
    private String requireAllowedColumn(
            String table,
            String column,
            boolean writeAccess
    ) {

        String validColumn = requireIdentifier(column);

        if (!enforceAllowlist) {
            return validColumn;
        }

        if (table == null) {

            throw new IllegalStateException(
                    "Table context is required for allowlist validation"
            );
        }

        TablePolicy policy = tablePolicies.get(
                table.toUpperCase(Locale.ROOT)
        );

        if (policy == null) {

            throw new IllegalArgumentException(
                    "Database table is not permitted"
            );
        }

        Set<String> allowedColumns = writeAccess
                ? policy.writableColumns()
                : policy.readableColumns();

        if (!allowedColumns.contains(
                validColumn.toUpperCase(Locale.ROOT)
        )) {

            log.warn(() ->
                    "[MessageSQLParser][requireAllowedColumn] "
                            + "Column rejected by allowlist"
                            + " | accessType="
                            + (writeAccess ? "WRITE" : "READ")
            );

            throw new IllegalArgumentException(
                    "Database column is not permitted"
            );
        }

        return validColumn;
    }

    // =========================================================
    // 6. COMPARISON OPERATOR RESOLUTION
    // =========================================================

    /**
     * Converts the JSON operators into SQL operators.
     *
     * Only explicitly supported operators are accepted.
     *
     * The returned operator is a hardcoded SQL token,
     * never arbitrary client-provided SQL.
     */
    private String comparatorOperatorResolver(
            String operator
    ) {

        if (operator == null) {

            throw new IllegalArgumentException(
                    "Comparison operator cannot be null"
            );
        }

        return switch (operator) {

            case "EQ" -> "=";

            case "GE" -> ">=";

            case "GT" -> ">";

            case "LE" -> "<=";

            case "LT" -> "<";

            case "NE" -> "<>";

            case "LIKE" -> "LIKE";

            default -> {

                log.warn(
                        "[MessageSQLParser][comparatorOperatorResolver] "
                                + "Unsupported comparison operator"
                );

                throw new IllegalArgumentException(
                        "Unsupported comparison operator"
                );
            }
        };
    }

    // =========================================================
    // 7. PARAMETER EXTRACTION
    // =========================================================

    /**
     * Converts a JSON scalar value into a Java value
     * suitable for parameterized SQL.
     *
     * Supported:
     *
     * JSON number  -> Number
     * JSON boolean -> Boolean
     * JSON string  -> String
     *
     * Unsupported:
     *
     * JSON null
     * JSON object
     * JSON array
     *
     * SQL NULL requires explicit handling using
     * IS NULL / IS NOT NULL, which is not implemented
     * in the current query protocol.
     */
    private Object extractValue(
            JsonNode value
    ) {

        if (value == null || value.isNull()) {

            log.warn(
                    "[MessageSQLParser][extractValue] "
                            + "Null SQL parameter rejected"
            );

            throw new IllegalArgumentException(
                    "NULL values are not supported"
            );
        }

        if (value.isNumber()) {

            /*
             * Bound the textual numeric representation
             * before converting it to a JDBC parameter.
             */
            if (value.asText().length() > MAX_TEXT_LENGTH) {

                throw new IllegalArgumentException(
                        "Numeric parameter is too large"
                );
            }

            return value.numberValue();
        }

        if (value.isBoolean()) {
            return value.booleanValue();
        }

        if (value.isTextual()) {

            String text = value.textValue();

            if (text.length() > MAX_TEXT_LENGTH) {

                throw new IllegalArgumentException(
                        "Text parameter exceeds maximum length"
                );
            }

            return text;
        }

        log.warn(() ->
                "[MessageSQLParser][extractValue] "
                        + "Unsupported parameter type"
                        + " | nodeType=" + value.getNodeType()
        );

        throw new IllegalArgumentException(
                "Unsupported SQL parameter type"
        );
    }

    // =========================================================
    // 8. FILTER COMPLEXITY TRACKING
    // =========================================================

    /**
     * Tracks the number of filter nodes processed
     * for a single query.
     *
     * This object is created per request.
     *
     * It is not shared between threads and therefore
     * does not require synchronization.
     */
    private static final class FilterBudget {

        private int nodes;

        private void recordNode(
                int depth
        ) {

            if (depth > MAX_FILTER_DEPTH) {

                throw new IllegalArgumentException(
                        "Maximum filter nesting depth exceeded"
                );
            }

            nodes++;

            if (nodes > MAX_FILTER_NODES) {

                throw new IllegalArgumentException(
                        "Maximum filter condition count exceeded"
                );
            }
        }
    }

    // =========================================================
    // 9. RECURSIVE FILTER PROCESSING
    // =========================================================

    /**
     * Preserves the original public method.
     *
     * This overload is intended for legacy callers.
     *
     * Restricted parsers require a table context
     * to enforce column allowlists, so their filters
     * must be built using createSQLFromMessage().
     */
    public void processCondition(
            JsonNode condition,
            StringBuilder sql,
            List<Object> parameterList
    ) {

        Objects.requireNonNull(sql);
        Objects.requireNonNull(parameterList);

        if (enforceAllowlist) {

            throw new IllegalStateException(
                    "Restricted filters require a table context"
            );
        }

        processCondition(
                condition,
                sql,
                parameterList,
                null,
                new FilterBudget(),
                1
        );
    }

    /**
     * Recursively converts a JSON filter expression
     * into parameterized SQL.
     *
     * Every expression must contain exactly one key:
     *
     * 1. AND
     * 2. OR
     * 3. A column name
     *
     * Examples:
     *
     * {"STATUS": {"operator": "EQ", "value": "OK"}}
     *
     * {
     *   "AND": [
     *     {"STATUS": {"operator": "EQ", "value": "OK"}},
     *     {"AMOUNT": {"operator": "GT", "value": 200}}
     *   ]
     * }
     *
     * Groups can contain other groups recursively.
     */
    private void processCondition(
            JsonNode condition,
            StringBuilder sql,
            List<Object> parameterList,
            String table,
            FilterBudget budget,
            int depth
    ) {

        // -----------------------------------------------------
        // VALIDATE FILTER STRUCTURE
        // -----------------------------------------------------

        budget.recordNode(depth);

        if (condition == null
                || !condition.isObject()
                || condition.size() != 1) {

            log.warn(
                    "[MessageSQLParser][processCondition] "
                            + "Invalid filter expression structure"
            );

            throw new IllegalArgumentException(
                    "Each filter expression must contain exactly one condition or logical group"
            );
        }

        Map.Entry<String, JsonNode> entry =
                condition.properties()
                        .iterator()
                        .next();

        String key = entry.getKey();
        JsonNode value = entry.getValue();

        // -----------------------------------------------------
        // PROCESS LOGICAL GROUP
        // -----------------------------------------------------

        if ("AND".equals(key) || "OR".equals(key)) {

            if (!value.isArray() || value.isEmpty()) {

                log.warn(() ->
                        "[MessageSQLParser][processCondition] "
                                + "Invalid logical group"
                                + " | operator=" + key
                );

                throw new IllegalArgumentException(
                        key + " requires a non-empty array"
                );
            }

            if (value.size() > MAX_FILTER_NODES) {

                throw new IllegalArgumentException(
                        "Too many conditions in logical group"
                );
            }

            log.debug(() ->
                    "[MessageSQLParser][processCondition] "
                            + "Processing logical group"
                            + " | operator=" + key
                            + " | children=" + value.size()
                            + " | depth=" + depth
            );

            sql.append("(");

            for (int i = 0; i < value.size(); i++) {

                if (i > 0) {

                    sql.append(" ")
                            .append(key)
                            .append(" ");
                }

                /*
                 * Recursive call:
                 *
                 * Each child can be a simple condition
                 * or another AND/OR group.
                 */
                processCondition(
                        value.get(i),
                        sql,
                        parameterList,
                        table,
                        budget,
                        depth + 1
                );
            }

            sql.append(")");

            return;
        }

        // -----------------------------------------------------
        // PROCESS INDIVIDUAL COLUMN CONDITION
        // -----------------------------------------------------

        /*
         * A column condition must have this structure:
         *
         * {
         *   "AMOUNT": {
         *     "operator": "GT",
         *     "value": 200
         *   }
         * }
         */
        String column = requireAllowedColumn(
                table,
                key,
                false
        );

        if (!value.isObject() || value.size() != 2) {

            throw new IllegalArgumentException(
                    "Invalid column condition structure"
            );
        }

        JsonNode operatorNode =
                value.path("operator");

        JsonNode valueNode =
                value.path("value");

        if (!operatorNode.isTextual()
                || valueNode.isMissingNode()) {

            throw new IllegalArgumentException(
                    "Column condition requires operator and value"
            );
        }

        String operator =
                comparatorOperatorResolver(
                        operatorNode.textValue()
                );

        /*
         * LIKE requires a textual parameter.
         *
         * SQL wildcard characters such as % and _
         * remain part of the bound parameter.
         */
        if ("LIKE".equals(operator)
                && !valueNode.isTextual()) {

            throw new IllegalArgumentException(
                    "LIKE requires a string value"
            );
        }

        if (parameterList.size() >= MAX_PARAMETERS) {

            throw new IllegalArgumentException(
                    "Maximum SQL parameter count exceeded"
            );
        }

        /*
         * The column and operator are validated.
         *
         * The value is never concatenated into SQL.
         * It is stored separately for PreparedStatement.
         */
        Object parameter = extractValue(valueNode);

        sql.append(column)
                .append(" ")
                .append(operator)
                .append(" ?");

        parameterList.add(parameter);
    }

    // =========================================================
    // 10. WHERE CLAUSE GENERATION
    // =========================================================

    /**
     * Builds a WHERE clause from the filters object.
     *
     * Missing or empty filters produce no WHERE clause.
     *
     * UPDATE and DELETE separately require a non-empty
     * WHERE clause.
     */
    private SQLQuery processWhere(
            JsonNode payload,
            String table
    ) {

        JsonNode filters = payload.path("filters");

        if (filters.isMissingNode() || filters.isNull()) {

            log.debug(
                    "[MessageSQLParser][processWhere] "
                            + "No filters supplied"
            );

            return new SQLQuery("", List.of());
        }

        if (!filters.isObject()) {

            throw new IllegalArgumentException(
                    "Filters must be a JSON object"
            );
        }

        if (filters.isEmpty()) {

            return new SQLQuery("", List.of());
        }

        StringBuilder sql =
                new StringBuilder(" WHERE ");

        List<Object> parameters =
                new ArrayList<>();

        processCondition(
                filters,
                sql,
                parameters,
                table,
                new FilterBudget(),
                1
        );

        SQLQuery result = new SQLQuery(
                sql.toString(),
                List.copyOf(parameters)
        );

        log.debug(() ->
                "[MessageSQLParser][processWhere] "
                        + "WHERE clause generated"
                        + " | parameterCount="
                        + result.parameters().size()
                        + " | queryLength="
                        + result.query().length()
        );

        return result;
    }

    // =========================================================
    // 11. SELECT / FETCH GENERATION
    // =========================================================

    /**
     * Generates a SELECT statement.
     *
     * If columns are omitted:
     *
     * Legacy mode:
     *     SELECT *
     *
     * Restricted mode:
     *     Select explicitly allowlisted read columns.
     *
     * Restricted mode never expands to unrestricted '*'.
     */
    private SQLQuery processFetch(
            JsonNode payload,
            String table
    ) {

        JsonNode columns =
                payload.path("columns");

        List<String> selectedColumns =
                new ArrayList<>();

        // -----------------------------------------------------
        // NO EXPLICIT COLUMNS
        // -----------------------------------------------------

        if (columns.isMissingNode()
                || columns.isNull()
                || (columns.isArray() && columns.isEmpty())) {

            if (enforceAllowlist) {

                TablePolicy policy = tablePolicies.get(
                        table.toUpperCase(Locale.ROOT)
                );

                selectedColumns.addAll(
                        policy.readableColumns()
                );

                selectedColumns.sort(String::compareTo);

                if (selectedColumns.isEmpty()
                        || selectedColumns.size() > MAX_SELECTED_COLUMNS) {

                    throw new IllegalArgumentException(
                            "No valid readable columns available"
                    );
                }

            } else {

                selectedColumns.add("*");
            }

        } else {

            // -------------------------------------------------
            // EXPLICIT COLUMN LIST
            // -------------------------------------------------

            if (!columns.isArray()) {

                throw new IllegalArgumentException(
                        "Columns must be a JSON array"
                );
            }

            if (columns.size() > MAX_SELECTED_COLUMNS) {

                throw new IllegalArgumentException(
                        "Too many selected columns"
                );
            }

            for (JsonNode column : columns) {

                if (!column.isTextual()) {

                    throw new IllegalArgumentException(
                            "Selected column must be a string"
                    );
                }

                selectedColumns.add(
                        requireAllowedColumn(
                                table,
                                column.textValue(),
                                false
                        )
                );
            }
        }

        String sql =
                "SELECT "
                        + String.join(", ", selectedColumns)
                        + " FROM "
                        + table;

        log.debug(() ->
                "[MessageSQLParser][processFetch] "
                        + "SELECT statement generated"
                        + " | table=" + table
                        + " | selectedColumns="
                        + selectedColumns.size()
        );

        return new SQLQuery(sql, List.of());
    }

    // =========================================================
    // 12. SEARCH GENERATION
    // =========================================================

    /**
     * Generates SELECT with optional WHERE filters.
     *
     * Reuses processFetch() and processWhere().
     */
    private SQLQuery processSearch(
            JsonNode payload,
            String table
    ) {

        SQLQuery select =
                processFetch(payload, table);

        SQLQuery where =
                processWhere(payload, table);

        String sql =
                select.query() + where.query();

        log.debug(() ->
                "[MessageSQLParser][processSearch] "
                        + "SEARCH statement generated"
                        + " | table=" + table
                        + " | parameterCount="
                        + where.parameters().size()
        );

        return new SQLQuery(
                sql,
                List.copyOf(where.parameters())
        );
    }

    // =========================================================
    // 13. INSERT / CREATE GENERATION
    // =========================================================

    /**
     * Generates a parameterized INSERT statement.
     *
     * Example:
     *
     * {
     *   "entity": "AGENT",
     *   "data": {
     *     "NAME": "EC01",
     *     "STATUS": "ACTIVE"
     *   }
     * }
     *
     * Result:
     *
     * INSERT INTO AGENT (NAME, STATUS)
     * VALUES (?, ?)
     */
    private SQLQuery processCreate(
            JsonNode payload,
            String table
    ) {

        JsonNode data = payload.path("data");

        if (!data.isObject() || data.isEmpty()) {

            log.warn(
                    "[MessageSQLParser][processCreate] "
                            + "CREATE rejected: missing data object"
            );

            throw new IllegalArgumentException(
                    "CREATE requires a non-empty data object"
            );
        }

        if (data.size() > MAX_DATA_FIELDS
                || data.size() > MAX_PARAMETERS) {

            throw new IllegalArgumentException(
                    "Too many CREATE data fields"
            );
        }

        List<String> columns =
                new ArrayList<>();

        List<Object> parameters =
                new ArrayList<>();

        Iterator<Map.Entry<String, JsonNode>> fields =
                data.properties().iterator();

        while (fields.hasNext()) {

            Map.Entry<String, JsonNode> field =
                    fields.next();

            String column =
                    requireAllowedColumn(
                            table,
                            field.getKey(),
                            true
                    );

            columns.add(column);

            parameters.add(
                    extractValue(field.getValue())
            );
        }

        String placeholders = String.join(
                ", ",
                java.util.Collections.nCopies(
                        columns.size(),
                        "?"
                )
        );

        String sql =
                "INSERT INTO "
                        + table
                        + " ("
                        + String.join(", ", columns)
                        + ") VALUES ("
                        + placeholders
                        + ")";

        log.debug(() ->
                "[MessageSQLParser][processCreate] "
                        + "INSERT statement generated"
                        + " | table=" + table
                        + " | fieldCount=" + columns.size()
                        + " | parameterCount=" + parameters.size()
        );

        return new SQLQuery(
                sql,
                List.copyOf(parameters)
        );
    }

    // =========================================================
    // 14. UPDATE GENERATION
    // =========================================================

    /**
     * Generates a parameterized UPDATE statement.
     *
     * A WHERE clause is mandatory.
     *
     * This protects against accidentally updating
     * every row in a table.
     */
    private SQLQuery processUpdate(
            JsonNode payload,
            String table
    ) {

        JsonNode data = payload.path("data");

        if (!data.isObject() || data.isEmpty()) {

            log.warn(
                    "[MessageSQLParser][processUpdate] "
                            + "UPDATE rejected: missing data object"
            );

            throw new IllegalArgumentException(
                    "UPDATE requires a non-empty data object"
            );
        }

        if (data.size() > MAX_DATA_FIELDS) {

            throw new IllegalArgumentException(
                    "Too many UPDATE fields"
            );
        }

        List<String> assignments =
                new ArrayList<>();

        List<Object> parameters =
                new ArrayList<>();

        Iterator<Map.Entry<String, JsonNode>> fields =
                data.properties().iterator();

        // -----------------------------------------------------
        // BUILD SET CLAUSE
        // -----------------------------------------------------

        while (fields.hasNext()) {

            Map.Entry<String, JsonNode> field =
                    fields.next();

            String column =
                    requireAllowedColumn(
                            table,
                            field.getKey(),
                            true
                    );

            assignments.add(column + " = ?");

            parameters.add(
                    extractValue(field.getValue())
            );
        }

        // -----------------------------------------------------
        // REQUIRE WHERE CLAUSE
        // -----------------------------------------------------

        SQLQuery where =
                processWhere(payload, table);

        if (where.query().isBlank()) {

            log.warn(
                    "[MessageSQLParser][processUpdate] "
                            + "UPDATE rejected: WHERE clause required"
            );

            throw new IllegalArgumentException(
                    "UPDATE requires filters"
            );
        }

        parameters.addAll(
                where.parameters()
        );

        if (parameters.size() > MAX_PARAMETERS) {

            throw new IllegalArgumentException(
                    "Maximum SQL parameter count exceeded"
            );
        }

        String sql =
                "UPDATE "
                        + table
                        + " SET "
                        + String.join(", ", assignments)
                        + where.query();

        log.debug(() ->
                "[MessageSQLParser][processUpdate] "
                        + "UPDATE statement generated"
                        + " | table=" + table
                        + " | updatedColumns="
                        + assignments.size()
                        + " | parameterCount="
                        + parameters.size()
        );

        return new SQLQuery(
                sql,
                List.copyOf(parameters)
        );
    }

    // =========================================================
    // 15. DELETE GENERATION
    // =========================================================

    /**
     * Generates DELETE with a mandatory WHERE clause.
     *
     * An unfiltered DELETE is rejected.
     */
    private SQLQuery processDelete(
            JsonNode payload,
            String table
    ) {

        SQLQuery where =
                processWhere(payload, table);

        if (where.query().isBlank()) {

            log.warn(
                    "[MessageSQLParser][processDelete] "
                            + "DELETE rejected: WHERE clause required"
            );

            throw new IllegalArgumentException(
                    "DELETE requires filters"
            );
        }

        String sql =
                "DELETE FROM "
                        + table
                        + where.query();

        log.debug(() ->
                "[MessageSQLParser][processDelete] "
                        + "DELETE statement generated"
                        + " | table=" + table
                        + " | parameterCount="
                        + where.parameters().size()
        );

        return new SQLQuery(
                sql,
                List.copyOf(where.parameters())
        );
    }

    // =========================================================
    // 16. FINAL QUERY VALIDATION
    // =========================================================

    /**
     * Checks final SQL size and parameter limits.
     *
     * Do not log the generated SQL or parameter values
     * here, because database requests may contain
     * account details or password hashes.
     */
    private SQLQuery validateQuery(
            SQLQuery query,
            Operation operation
    ) {

        if (query == null
                || query.query() == null
                || query.query().isBlank()) {

            throw new IllegalArgumentException(
                    "Generated SQL query is empty"
            );
        }

        if (query.query().length() > MAX_SQL_LENGTH) {

            throw new IllegalArgumentException(
                    "Generated SQL exceeds maximum length"
            );
        }

        if (query.parameters() == null
                || query.parameters().size() > MAX_PARAMETERS) {

            throw new IllegalArgumentException(
                    "Generated SQL exceeds parameter limit"
            );
        }

        log.debug(() ->
                "[MessageSQLParser][validateQuery] "
                        + "SQL query validated"
                        + " | operation=" + operation
                        + " | sqlLength=" + query.query().length()
                        + " | parameterCount="
                        + query.parameters().size()
        );

        return query;
    }

    // =========================================================
    // 17. MAIN MESSAGE-TO-SQL ENTRY POINT
    // =========================================================

    /**
     * Creates a SQLQuery from the Mediation Engine Message.
     *
     * Message.operation determines the SQL operation.
     *
     * Message.payload contains the JSON query structure.
     *
     * The method:
     *
     * 1. Validates the message and payload.
     * 2. Checks the table and operation policy.
     * 3. Generates the parameterized SQL.
     * 4. Validates the final query size.
     * 5. Returns SQLQuery for DatabaseService.
     *
     * Invalid requests throw IllegalArgumentException.
     *
     * DatabaseRequestHandler should translate these
     * exceptions into the standard error Message.
     */
    public SQLQuery createSQLFromMessage(
            Message message
    ) {

        Objects.requireNonNull(
                message,
                "Database message cannot be null"
        );

        Operation operation =
                message.getOperation();

        JsonNode payload =
                message.getPayload();

        if (operation == null) {

            throw new IllegalArgumentException(
                    "Database operation cannot be null"
            );
        }

        if (payload == null
                || !payload.isObject()) {

            log.warn(
                    "[MessageSQLParser][createSQLFromMessage] "
                            + "Invalid database payload"
            );

            throw new IllegalArgumentException(
                    "Database payload must be a JSON object"
            );
        }

        log.debug(() ->
                "[MessageSQLParser][createSQLFromMessage] "
                        + "Processing database request"
                        + " | operation=" + operation
        );

        // -----------------------------------------------------
        // VALIDATE OPERATION BEFORE TABLE ACCESS
        // -----------------------------------------------------

        if (operation != Operation.FETCH
                && operation != Operation.SEARCH
                && operation != Operation.CREATE
                && operation != Operation.UPDATE
                && operation != Operation.DELETE) {

            log.warn(() ->
                    "[MessageSQLParser][createSQLFromMessage] "
                            + "Unsupported database operation"
                            + " | operation=" + operation
            );

            throw new IllegalArgumentException(
                    "Unsupported database operation: " + operation
            );
        }

        // -----------------------------------------------------
        // RESOLVE TABLE AND CHECK ALLOWLIST
        // -----------------------------------------------------

        String table =
                requireEntity(payload, operation);

        // -----------------------------------------------------
        // DISPATCH TO SQL GENERATOR
        // -----------------------------------------------------

        SQLQuery query = switch (operation) {

            case FETCH ->
                    processFetch(payload, table);

            case SEARCH ->
                    processSearch(payload, table);

            case CREATE ->
                    processCreate(payload, table);

            case UPDATE ->
                    processUpdate(payload, table);

            case DELETE ->
                    processDelete(payload, table);

            default -> throw new IllegalArgumentException(
                    "Unsupported database operation"
            );
        };

        // -----------------------------------------------------
        // VALIDATE AND RETURN GENERATED SQL
        // -----------------------------------------------------

        SQLQuery validatedQuery =
                validateQuery(query, operation);

        log.debug(() ->
                "[MessageSQLParser][createSQLFromMessage] "
                        + "Database request parsed successfully"
                        + " | operation=" + operation
                        + " | table=" + table
                        + " | parameterCount="
                        + validatedQuery.parameters().size()
        );

        return validatedQuery;
    }
}
