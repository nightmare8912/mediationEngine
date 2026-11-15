package org.prats.database.databaseUtilities.sqlHelper;

import org.prats.definitions.records.Table;
import org.prats.definitions.records.TableField;
import org.prats.loggers.EngineLogger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SQLSyntaxHelper {

    private static final EngineLogger log =
            EngineLogger.getLogger(SQLSyntaxHelper.class);

    public SQLQuery createTableStatementHelper(Table table) {

        log.debug(() ->
                "[SQLSyntaxHelper][createTableStatementHelper] Generating CREATE TABLE statement"
                        + " | tableId=" + table.id()
                        + " | fieldCount=" + table.numberOfFields()
        );

        // empty sql
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ")
                .append(table.id())
                .append(" (");

        for (TableField tableField : table.fields())
        {
            sql.append(tableField.id())
                    .append(" ")
                    .append(tableField.type());

            if (tableField.required())
                sql.append(" NOT NULL");

            sql.append(", ");
        }

        List<String> primaryKeysList = new ArrayList<>();

        for (TableField field : table.fields())
        {
            if (field.primaryKey())
                primaryKeysList.add(field.id());
        }

        if (!primaryKeysList.isEmpty())
        {
            sql.append(" PRIMARY KEY (")
                    .append(String.join(", ", primaryKeysList))
                    .append(")");
        }

        sql.append(")");

        SQLQuery sqlQuery = new SQLQuery(sql.toString(), List.of());

        log.debug(() ->
                "[SQLSyntaxHelper][createTableStatementHelper] CREATE TABLE statement generated"
                        + " | tableId=" + table.id()
                        + " | fieldCount=" + table.numberOfFields()
                        + " | primaryKeyCount=" + primaryKeysList.size()
        );

        return sqlQuery;
    }

    public SQLQuery dropTableStatementHelper(Table table) {

        log.debug(() ->
                "[SQLSyntaxHelper][dropTableStatementHelper] Generating DROP TABLE statement"
                        + " | tableId=" + table.id()
        );

        // empty sql
        StringBuilder sql = new StringBuilder();
        sql.append("DROP TABLE IF EXISTS ")
                .append(table.id());

        SQLQuery sqlQuery = new SQLQuery(sql.toString(), List.of());

        log.debug(() ->
                "[SQLSyntaxHelper][dropTableStatementHelper] DROP TABLE statement generated"
                        + " | tableId=" + table.id()
        );

        return sqlQuery;
    }

    public SQLQuery insertTableStatementHelper(
            Table table,
            Map<String, String> record
    ) {

        log.debug(() ->
                "[SQLSyntaxHelper][insertTableStatementHelper] Generating INSERT statement"
                        + " | tableId=" + table.id()
                        + " | fieldCount=" + table.numberOfFields()
                        + " | recordFieldCount=" + record.size()
        );

        StringBuilder sql = new StringBuilder();

        List<Object> parameterList = new ArrayList<>();

        sql.append("INSERT INTO ")
                .append(table.id())
                .append(" ( ")
                .append(String.join(", ",
                        table.fields().stream()
                                .map(TableField::id)
                                .toList()))
                .append(" ) VALUES ( ")
                .append(String.join(", ",
                        Collections.nCopies(table.numberOfFields(), "?")))
                .append(" )");

        for (TableField field : table.fields()) {
            parameterList.add(record.get(field.id()));
        }

        SQLQuery sqlQuery = new SQLQuery(sql.toString(), parameterList);

        log.debug(() ->
                "[SQLSyntaxHelper][insertTableStatementHelper] INSERT statement generated"
                        + " | tableId=" + table.id()
                        + " | parameterCount=" + parameterList.size()
        );

        return sqlQuery;
    }
}