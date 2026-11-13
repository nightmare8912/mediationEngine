package org.prats.database.databaseUtilities;

import com.fasterxml.jackson.databind.JsonNode;
import org.prats.database.databaseUtilities.sqlHelper.SQLQuery;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.Operation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MessageSQLParser {
    /*
        typical format ... yeah this is complex. at-least for me
        the parser should take it and make something like
        Select col1, col2 from AGENT where fieldName1 = ‘OK’ AND fieldName2 > 200 AND ( fieldName3 < 300 OR fieldName4 LIKE ‘OK’
        {
          "entity": "AGENT",
          "columns: ["col1", "col2"]
          "filters": {
                “AND”: [
                    “fieldName1”: {
                        “operator”: “EQ”,
                        “value”	 : “OK”
                    },
                    “fieldName2”: {
                        “operator”: “GT”
                        “value”   : 200
                    },
                    {
                        “OR”: [
                            {
                                “fieldName3”: {
                                    “operator”: “LT”,
                                    “value”	 : 300
                                }
                            },
                            {
                                “fieldName4”: {
                                    “operator”: “LIKE”,
                                    “value”: “OK”
                                }
                            }
                        ]
                    }
                ],
                “OR”:  []
          }
        }
     */

    private String comparatorOperatorResolver(String operator) {
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
                return "NE";
            }
            default -> {
                return "Invalid Operator";
            }
        }
    }

    private SQLQuery processFetch(JsonNode payload) {

        StringBuilder sql = new StringBuilder("SELECT ");

        JsonNode columns = payload.path("columns");

        if (columns.isMissingNode() || columns.isNull() || columns.isEmpty()) {
            sql.append("*");
        } else {
            List<String> columnNames = new ArrayList<>();

            for (JsonNode column : columns) {
                columnNames.add(column.asText());
            }

            sql.append(String.join(", ", columnNames));
        }

        JsonNode tableName = payload.path("entity");

        if (tableName.isMissingNode() || tableName.isNull() || tableName.isEmpty()) {
            return processInvalid(payload);
        }

        sql.append(" FROM ").append(tableName.asText());

        return new SQLQuery(sql.toString(), List.of());
    }

    private SQLQuery processWhere(JsonNode payload) {

        StringBuilder sql = new StringBuilder();
        List<Object> parameterList = new ArrayList<>();

        JsonNode filters = payload.path("filters");

        if (filters.isMissingNode() || filters.isNull() || filters.isEmpty()) {
            return new SQLQuery("", parameterList);
        }

        sql.append(" WHERE ");

        processCondition(filters, sql, parameterList);

        return new SQLQuery(sql.toString(), parameterList);
    }

    public void processCondition(
            JsonNode condition,
            StringBuilder sql,
            List<Object> parameterList) {

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

                // we encountered an AND/OR. The corresponding value would be
                // a list (by design), so we iterate over it.

                if (!value.isArray() || value.isEmpty()) {
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

            } else {
                // this is the case where we process the operator, so no more recursion as this is expected to be a plain
                // structure
                String columnName = key;

                JsonNode operatorNode = value.path("operator");

                JsonNode valueNode = value.path("value");

                if (!operatorNode.isTextual() || valueNode.isMissingNode()) {

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
            }
        }
    }

    private Object extractValue(JsonNode value) {

        if (value.isNull()) {
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

        throw new IllegalArgumentException(
                "Unsupported filter value: " + value
        );
    }

    private SQLQuery processSearch(JsonNode payload) {
        StringBuilder sql = new StringBuilder();

        sql.append(processFetch(payload).query());

        SQLQuery whereClause = processWhere(payload);
        sql.append(whereClause.query());

        return new SQLQuery(sql.toString(), whereClause.parameters());
    }

    private SQLQuery processUpdate(JsonNode payload) {
        StringBuilder sql           = new StringBuilder();
        List<Object> parameterList  = new ArrayList<>();

        sql.append("UPDATE ");

        JsonNode tableName = payload.path("entity");

        if (tableName.isMissingNode() || tableName.isNull() || tableName.isEmpty())
            return processInvalid(payload);

        sql.append(tableName.asText())
                .append(" SET ");

        JsonNode data = payload.path("data");

        if (!data.isObject() || data.isEmpty())
            return processInvalid(payload);

        Iterator<Map.Entry<String, JsonNode>> fields = data.properties().iterator();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();

            String columnName   = field.getKey();
            JsonNode valueNode  = field.getValue();

            sql.append(columnName)
                    .append(" = ? ");

            if (fields.hasNext())
                sql.append(", ");

            parameterList.add(extractValue(valueNode));
        }

        SQLQuery whereClause = processWhere(payload);

        if (whereClause.query().isEmpty())
            return processInvalid(payload);

        sql.append(whereClause.query());
        parameterList.addAll(whereClause.parameters());

        return new SQLQuery(sql.toString(), parameterList);
    }

    private SQLQuery processDelete(JsonNode payload) {
        StringBuilder sql = new StringBuilder();

        // process the operation

        return null;
    }

    private SQLQuery processCreate(JsonNode payload) {
        StringBuilder sql = new StringBuilder();

        // process the operation

        return null;
    }

    private SQLQuery processInvalid(JsonNode payload) {
        StringBuilder sql = new StringBuilder();

        // process the operation

        return null;
    }

    public SQLQuery createSQLFromMessage(
            Message message
    ) {

        Operation operation     = message.getOperation();
        JsonNode payload        = message.getPayload();

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
                return processInvalid(payload);
            }
        }
    }
}
