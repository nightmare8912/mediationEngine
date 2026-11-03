package org.prats.database.databaseUtilities.sqlHelper;

import org.prats.definitions.Table;
import org.prats.definitions.TableField;

import java.util.ArrayList;
import java.util.List;

public class SQLSyntaxHelper {
    public String createTableStatementHelper(Table table) {
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

        return sql.toString();
    }

    private String dropTableStatementHelper(Table table) {
        // empty sql
        StringBuilder sql = new StringBuilder();
        sql.append("DROP TABLE IF EXISTS ")
                .append(table.id());
        return sql.toString();
    }
}
