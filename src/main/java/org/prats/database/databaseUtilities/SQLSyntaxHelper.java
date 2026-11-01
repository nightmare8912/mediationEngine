package org.prats.database.databaseUtilities;

import org.prats.definitions.TableDefinition;
import org.prats.definitions.TableFieldDefinition;

import java.util.ArrayList;
import java.util.List;

public class SQLSyntaxHelper {
    public String createTableStatementHelper(TableDefinition tableDefinition) {
        // empty sql
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ")
                .append(tableDefinition.getId())
                .append(" (");

        for (TableFieldDefinition tableFieldDefinition: tableDefinition.getFields())
        {
            sql.append(tableFieldDefinition.getId())
                    .append(" ")
                    .append(tableFieldDefinition.getType());

            if (tableFieldDefinition.getRequired())
                sql.append(" NOT NULL");

            sql.append(", ");
        }

        List<String> primaryKeysList = new ArrayList<>();

        for (TableFieldDefinition field : tableDefinition.getFields())
        {
            if (field.getPrimaryKey())
                primaryKeysList.add(field.getId());
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

    private String dropTableStatementHelper(TableDefinition tableDefinition) {
        // empty sql
        StringBuilder sql = new StringBuilder();
        sql.append("DROP TABLE IF EXISTS ")
                .append(tableDefinition.getId());
        return sql.toString();
    }
}
