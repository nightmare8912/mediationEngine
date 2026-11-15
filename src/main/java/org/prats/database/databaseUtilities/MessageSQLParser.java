package org.prats.database.databaseUtilities;

import com.fasterxml.jackson.databind.JsonNode;
import org.prats.database.databaseUtilities.sqlHelper.SQLQuery;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.Operation;
import org.prats.loggers.EngineLogger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class MessageSQLParser {

    private static final Pattern SQL_IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private static final EngineLogger log =
            EngineLogger.getLogger(MessageSQLParser.class);

    /**
     * typical format ... yeah this is complex. at-least for me
     * the parser should take it and make something like
     * Select col1, col2 from AGENT where fieldName1 = ‘OK’ AND fieldName2 > 200 AND ( fieldName3 < 300 OR fieldName4 LIKE ‘OK’
     * {
     *   "entity": "AGENT",
     *   "columns: ["col1", "col2"]
     *   "filters": {
     *         “AND”: [
     *             “fieldName1”: {
     *                 “operator”: “EQ”,
     *                 “value”  : “OK”
     *             },
     *             “fieldName2”: {
     *                 “operator”: “GT”
     *                 “value”   : 200
     *             },
     *             {
     *                 “OR”: [
     *                     {
     *                         “fieldName3”: {
     *                             “operator”: “LT”,
     *                             “value”  : 300
     *                         }
     *                     },
     *                     {
     *                         “fieldName4”: {
     *                             “operator”: “LIKE”,
     *                             “value”: “OK”
     *                         }
     *                     }
     *                 ]
     *             }
     *         ],
     *         “OR”: []
     *   }
     * }
     */
    private String comparatorOperatorResolver(String operator) {

        log.debug(() ->
                "[MessageSQLParser][comparatorOperatorResolver] Resolving comparator operator"
                        + " | operator=" + operator
        );

        switch (operator) {
            case "EQ" -> {
                return "=";
            }
            case "GE" -> {
                return ">=";
            }
            case "GT" -> {
                return ">";
            }
            case "LE" -> {
                return "<=";
            }
            case "LT" -> {
                return "<";
            }
            case "NE" -> {
                return "<>";
            }
            default -> {
                log.warn(() ->
                        "[MessageSQLParser][comparatorOperatorResolver] Unsupported comparator operator"
                                + " | operator=" + operator
                );

                throw new IllegalArgumentException("Unsupported comparator operator");
            }
        }
    }

    private String requireIdentifier(String identifier) {
        if (identifier == null || !SQL_IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Invalid SQL identifier");
        }
        return identifier;
    }

    private SQLQuery processFetch(JsonNode payload) {

        log.debug(
                "[MessageSQLParser][processFetch] Processing FETCH operation"
        );

        StringBuilder sql = new StringBuilder("SELECT ");

        JsonNode columns = payload.path("columns");

        if (columns.isMissingNode() || columns.isNull() || columns.isEmpty()) {

            log.debug(
                    "[MessageSQLParser][processFetch] No columns specified; selecting all columns"
            );

            sql.append("*");

        } else {

            List<String> columnNames = new ArrayList<>();

            for (JsonNode column : columns) {
                columnNames.add(requireIdentifier(column.asText()));
            }

            sql.append(String.join(", ", columnNames));

            log.debug(() ->
                    "[MessageSQLParser][processFetch] Selected columns extracted"
                            + " | columnCount=" + columnNames.size()
            );
        }

        JsonNode tableName = payload.path("entity");

        if (tableName.isMissingNode() || tableName.isNull() || tableName.asText().isBlank()) {

            log.warn(
                    "[MessageSQLParser][processFetch] FETCH rejected because entity/table name is missing"
            );

            return processInvalid(payload);
        }

        sql.append(" FROM ").append(requireIdentifier(tableName.asText()));

        SQLQuery result = new SQLQuery(sql.toString(), List.of());

        log.debug(() ->
                "[MessageSQLParser][processFetch] FETCH SQL generated"
                        + " | table=" + tableName.asText()
                        + " | queryLength=" + result.query().length()
        );

        return result;
    }

    private SQLQuery processWhere(JsonNode payload) {

        log.debug(
                "[MessageSQLParser][processWhere] Processing WHERE clause"
        );

        StringBuilder sql = new StringBuilder();
        List<Object> parameterList = new ArrayList<>();

        JsonNode filters = payload.path("filters");

        if (filters.isMissingNode() || filters.isNull() || filters.isEmpty()) {

            log.debug(
                    "[MessageSQLParser][processWhere] No filters supplied; returning empty WHERE clause"
            );

            return new SQLQuery("", parameterList);
        }

        sql.append(" WHERE ");

        processCondition(filters, sql, parameterList);

        SQLQuery result = new SQLQuery(sql.toString(), parameterList);

        log.debug(() ->
                "[MessageSQLParser][processWhere] WHERE clause generated"
                        + " | parameterCount=" + parameterList.size()
                        + " | queryLength=" + result.query().length()
        );

        return result;
    }

    public void processCondition(
            JsonNode condition,
            StringBuilder sql,
            List<Object> parameterList
    ) {

        log.debug(() ->
                "[MessageSQLParser][processCondition] Processing filter condition"
                        + " | conditionFieldCount=" + condition.size()
                        + " | currentParameterCount=" + parameterList.size()
        );

        Iterator<Map.Entry<String, JsonNode>> fields =
                condition.properties().iterator();

        while (fields.hasNext()) {

            Map.Entry<String, JsonNode> field = fields.next();

            String key = field.getKey();

            JsonNode value = field.getValue();

            // now is the tricky part, since here it could be an actual condition
            // again (like AND or OR) in case of nested conditions,
            // or it could be a field name.
            if (key.equals("AND") || key.equals("OR")) {

                log.debug(() ->
                        "[MessageSQLParser][processCondition] Logical condition encountered"
                                + " | logicalOperator=" + key
                                + " | nestedConditionCount=" + value.size()
                );

                // we encountered an AND/OR. The corresponding value would be
                // a list (by design), so we iterate over it.
                if (!value.isArray() || value.isEmpty()) {

                    log.warn(() ->
                            "[MessageSQLParser][processCondition] Invalid logical condition"
                                    + " | logicalOperator=" + key
                                    + " | isArray=" + value.isArray()
                                    + " | isEmpty=" + value.isEmpty()
                    );

                    throw new IllegalArgumentException(
                            key + " must have a non-empty array."
                    );
                }

                sql.append("(");

                Iterator<JsonNode> conditions = value.elements();

                while (conditions.hasNext()) {

                    JsonNode nestedCondition = conditions.next();

                    processCondition(nestedCondition, sql, parameterList);

                    if (conditions.hasNext()) {
                        sql.append(" ").append(key).append(" ");
                    }
                }

                sql.append(")");

                log.debug(() ->
                        "[MessageSQLParser][processCondition] Logical condition processed"
                                + " | logicalOperator=" + key
                                + " | parameterCount=" + parameterList.size()
                );

            } else {
                // this is the case where we process the operator, so no more recursion as this is expected to be a plain
                // structure
                String columnName = requireIdentifier(key);

                JsonNode operatorNode = value.path("operator");

                JsonNode valueNode = value.path("value");

                if (!operatorNode.isTextual() || valueNode.isMissingNode()) {

                    log.warn(() ->
                            "[MessageSQLParser][processCondition] Invalid column condition"
                                    + " | columnName=" + columnName
                                    + " | operatorIsTextual=" + operatorNode.isTextual()
                                    + " | valueMissing=" + valueNode.isMissingNode()
                    );

                    throw new IllegalArgumentException(
                            "Invalid condition for column: " + columnName
                    );
                }

                String operator = comparatorOperatorResolver(
                        operatorNode.asText()
                );

                sql.append(columnName)
                        .append(" ")
                        .append(operator)
                        .append(" ?");

                parameterList.add(extractValue(valueNode));

                log.debug(() ->
                        "[MessageSQLParser][processCondition] Column condition processed"
                                + " | columnName=" + columnName
                                + " | operator=" + operator
                                + " | parameterCount=" + parameterList.size()
                );
            }
        }
    }

    private Object extractValue(JsonNode value) {

        log.debug(() ->
                "[MessageSQLParser][extractValue] Extracting SQL parameter"
                        + " | valueType="
                        + (value.isNull() ? "NULL"
                        : value.isNumber() ? "NUMBER"
                        : value.isBoolean() ? "BOOLEAN"
                        : value.isTextual() ? "TEXT"
                        : "UNSUPPORTED")
        );

        if (value.isNull()) {

            log.warn(
                    "[MessageSQLParser][extractValue] Null filter value rejected"
            );

            throw new IllegalArgumentException(
                    "NULL values should use IS NULL or IS NOT NULL"
            );
        }

        if (value.isNumber()) {
            return value.numberValue();
        }

        if (value.isBoolean()) {
            return value.booleanValue();
        }

        if (value.isTextual()) {
            return value.textValue();
        }

        log.warn(() ->
                "[MessageSQLParser][extractValue] Unsupported SQL parameter value type"
                        + " | nodeType=" + value.getNodeType()
        );

        throw new IllegalArgumentException(
                "Unsupported filter value type"
        );
    }

    private SQLQuery processSearch(JsonNode payload) {

        log.debug(
                "[MessageSQLParser][processSearch] Processing SEARCH operation"
        );

        StringBuilder sql = new StringBuilder();

        sql.append(processFetch(payload).query());

        SQLQuery whereClause = processWhere(payload);
        sql.append(whereClause.query());

        SQLQuery result = new SQLQuery(sql.toString(), whereClause.parameters());

        log.debug(() ->
                "[MessageSQLParser][processSearch] SEARCH SQL generated"
                        + " | queryLength=" + result.query().length()
                        + " | parameterCount=" + result.parameters().size()
        );

        return result;
    }

    private SQLQuery processUpdate(JsonNode payload) {

        log.debug(
                "[MessageSQLParser][processUpdate] Processing UPDATE operation"
        );

        StringBuilder sql = new StringBuilder();
        List<Object> parameterList = new ArrayList<>();

        sql.append("UPDATE ");

        JsonNode tableName = payload.path("entity");

        if (tableName.isMissingNode() || tableName.isNull() || tableName.asText().isBlank()) {

            log.warn(
                    "[MessageSQLParser][processUpdate] UPDATE rejected because entity/table name is missing"
            );

            return processInvalid(payload);
        }

        sql.append(requireIdentifier(tableName.asText()))
                .append(" SET ");

        JsonNode data = payload.path("data");

        if (!data.isObject() || data.isEmpty()) {

            log.warn(() ->
                    "[MessageSQLParser][processUpdate] UPDATE rejected because data is not a non-empty object"
                            + " | table=" + tableName.asText()
            );

            return processInvalid(payload);
        }

        Iterator<Map.Entry<String, JsonNode>> fields = data.properties().iterator();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();

            String columnName = requireIdentifier(field.getKey());
            JsonNode valueNode = field.getValue();

            sql.append(columnName)
                    .append(" = ? ");

            if (fields.hasNext())
                sql.append(", ");

            parameterList.add(extractValue(valueNode));
        }

        SQLQuery whereClause = processWhere(payload);

        if (whereClause.query().isEmpty()) {

            log.warn(() ->
                    "[MessageSQLParser][processUpdate] UPDATE rejected because WHERE clause is empty"
                            + " | table=" + tableName.asText()
            );

            return processInvalid(payload);
        }

        sql.append(whereClause.query());
        parameterList.addAll(whereClause.parameters());

        SQLQuery result = new SQLQuery(sql.toString(), parameterList);

        log.debug(() ->
                "[MessageSQLParser][processUpdate] UPDATE SQL generated"
                        + " | table=" + tableName.asText()
                        + " | parameterCount=" + parameterList.size()
                        + " | queryLength=" + result.query().length()
        );

        return result;
    }

    private SQLQuery processDelete(JsonNode payload) {
        String table = requireIdentifier(payload.path("entity").asText());
        SQLQuery where = processWhere(payload);
        if (where.query().isBlank()) throw new IllegalArgumentException("DELETE requires filters");
        return new SQLQuery("DELETE FROM " + table + where.query(), where.parameters());
    }

    private SQLQuery processCreate(JsonNode payload) {
        String table = requireIdentifier(payload.path("entity").asText());
        JsonNode data = payload.path("data");
        if (!data.isObject() || data.isEmpty()) throw new IllegalArgumentException("CREATE requires data fields");
        List<String> columns = new ArrayList<>(); List<Object> parameters = new ArrayList<>();
        data.properties().forEach(field -> {
            columns.add(requireIdentifier(field.getKey())); parameters.add(extractValue(field.getValue()));
        });
        return new SQLQuery("INSERT INTO " + table + " (" + String.join(", ", columns) + ") VALUES ("
                + String.join(", ", java.util.Collections.nCopies(columns.size(), "?")) + ")", parameters);
    }

    private SQLQuery processInvalid(JsonNode payload) {

        log.warn(
                "[MessageSQLParser][processInvalid] Invalid SQL operation or payload; returning existing placeholder result"
        );

        StringBuilder sql = new StringBuilder();

        // process the operation
        return null;
    }

    public SQLQuery createSQLFromMessage(
            Message message
    ) {

        Operation operation = message.getOperation();
        JsonNode payload = message.getPayload();

        log.debug(() ->
                "[MessageSQLParser][createSQLFromMessage] Creating SQL from incoming message"
                        + " | operation=" + operation
                        + " | payloadPresent=" + (payload != null && !payload.isNull())
        );

        switch (operation) {
            case FETCH -> {
                return processFetch(payload);
            }
            case SEARCH -> {
                return processSearch(payload);
            }
            case CREATE -> {
                return processCreate(payload);
            }
            case UPDATE -> {
                return processUpdate(payload);
            }
            case DELETE -> {
                return processDelete(payload);
            }
            default -> {
                log.warn(() ->
                        "[MessageSQLParser][createSQLFromMessage] Unsupported database operation"
                                + " | operation=" + operation
                );

                return processInvalid(payload);
            }
        }
    }
}
