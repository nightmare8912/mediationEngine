package org.prats.database.databaseUtilities.sqlHelper;

import org.apache.commons.csv.CSVRecord;
import org.prats.definitions.Table;
import org.prats.definitions.TableField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SQLSyntaxHelper {
    public SQLQuery createTableStatementHelper(Table table) {
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

        return new SQLQuery(sql.toString(), List.of());
    }

    public SQLQuery dropTableStatementHelper(Table table) {
        // empty sql
        StringBuilder sql = new StringBuilder();
        sql.append("DROP TABLE IF EXISTS ")
                .append(table.id());
        return new SQLQuery(sql.toString(), List.of());
    }

    public SQLQuery insertTableStatementHelper(
            Table table,
            Map<String, String> record
    ) {
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

        return new SQLQuery(sql.toString(), parameterList);
    }
}
