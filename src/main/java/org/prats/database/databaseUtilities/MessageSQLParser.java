package org.prats.database.databaseUtilities;

import com.fasterxml.jackson.databind.JsonNode;
import org.prats.database.databaseUtilities.sqlHelper.SQLQuery;
import org.prats.definitions.MessageEntity;
import org.prats.definitions.Operation;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MessageSQLParser {

    public MessageSQLParser() {}

    private SQLQuery processFetch(JsonNode payload) {
        StringBuilder sql = new StringBuilder();

        sql.append("SELECT ");

        JsonNode columns = payload.path("columns");
        if (columns.isMissingNode() || columns.isNull() || columns.isEmpty())
            sql.append("* ");
        else
        {
            for(JsonNode column: columns)
                sql.append(column.asText()).append(", ");

            // remove ending comma, with its extra space
            sql.setLength(sql.length() - 2);
        }

        JsonNode tableName = payload.path("entity");

        if (tableName.isMissingNode() || tableName.isNull() || tableName.isEmpty())
            return processInvalid(payload);

        sql.append(" FROM ").append(tableName.asText());

        return new SQLQuery(sql.toString(), List.of());
    }

    private SQLQuery processWhere(JsonNode payload) {
        StringBuilder sql = new StringBuilder();
        List<Object> parameterList = new ArrayList<>();

        JsonNode filters = payload.path("filters");
        if (!filters.isMissingNode() &&
                !filters.isNull() &&
                !filters.isEmpty())
        {
            sql.append(" WHERE ");

            Iterator<Map.Entry<String, JsonNode>> fields = filters.properties().iterator();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();

                String columnName   = field.getKey();
                JsonNode value      = field.getValue();

                sql.append(columnName).append(" = ? ");

                parameterList.add(
                        value.isNull() ? null :
                                value.isNumber() ? value.numberValue() :
                                        value.isBoolean() ? value.booleanValue() :
                                                value.asText()
                );

                if (fields.hasNext())
                    sql.append(" AND ");
            }
        }

        return new SQLQuery(sql.toString(), parameterList);
    }

    private SQLQuery processSearch(JsonNode payload) {
        StringBuilder sql = new StringBuilder();

        // process the operation

        return null;
    }

    private SQLQuery processUpdate(JsonNode payload) {
        StringBuilder sql = new StringBuilder();

        // process the operation

        return null;
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
            MessageEntity messageEntity
    ) {

        Operation operation     = messageEntity.getMessage().getOperation();
        JsonNode payload        = messageEntity.getMessage().getPayload();

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

    public MessageEntity createMessageEntityFromDBResponse(int dbResult) {
        return null; // pending
    }

    public MessageEntity createMessageEntityFromDBResponse(ResultSet resultSet) {
        return null; // pending
    }
}
