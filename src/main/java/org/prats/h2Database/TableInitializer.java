package org.prats.h2Database;

import org.prats.configParser.ConfigParser;
import org.prats.definitions.DatabaseDefinition;
import org.prats.definitions.DatabaseFieldDefinition;
import org.prats.definitions.TableDefinition;
import org.prats.definitions.TableFieldDefinition;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class TableInitializer {
    private String databaseConfigFile;

    public TableInitializer() {}

    public TableInitializer(String databaseConfigFile) {
        this.databaseConfigFile = databaseConfigFile;
    }

    private StringBuilder validateTable(TableDefinition tableDefinition) {
        boolean isError = false;
        StringBuilder errorMessage = new StringBuilder();

        if (tableDefinition.getLabel() == null || tableDefinition.getId().isEmpty())
        {
            errorMessage.append("Table ID is empty in file ");
            isError = true;
        }

        if (!isError && tableDefinition.getNumberOfFields() == 0)
        {
            errorMessage.append("No fields in the table");
            isError = true;
        }

        if (!isError)
        {
            for (TableFieldDefinition tableFieldDefinition : tableDefinition.getFields())
            {
                if (tableFieldDefinition.getId() == null || tableFieldDefinition.getId().isEmpty())
                {
                    errorMessage.append("Field name empty in table ");
                    isError = true; break;
                }
                if (tableFieldDefinition.getType() == null || tableFieldDefinition.getType().equals(""))
                {
                    errorMessage.append("Field type empty for field "); errorMessage.append(tableFieldDefinition.getId());
                    isError = true; break;
                }
            }
        }

        return errorMessage;
    }

    String generateCreateTableSQL(TableDefinition tableDefinition) {
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

    private StringBuilder createTable(Connection connection, TableDefinition tableDefinition) throws SQLException {

        StringBuilder errorMessage = validateTable(tableDefinition);

        if (errorMessage.isEmpty())
        {
            String sql = generateCreateTableSQL(tableDefinition);
            System.out.println(sql);
            try (Statement statement = connection.createStatement())
            {
                statement.execute(sql);
            } catch (SQLException sqle)
            {
                errorMessage.append("Error code: \n")
                                .append(sqle.getErrorCode())
                                .append("\nTrace: \n")
                                .append(Arrays.toString(sqle.getStackTrace()));
            }
        }
        return errorMessage;
    }

    public boolean initializeDatabase() {
        StringBuilder errorMessage;
        ConfigParser configParser = new ConfigParser();
        DatabaseDefinition databaseDefinition = configParser.initializeDatabaseConfig(databaseConfigFile);

        databaseDefinition.getTables().sort(
                new Comparator<DatabaseFieldDefinition>() {
                    @Override
                    public int compare(DatabaseFieldDefinition o1, DatabaseFieldDefinition o2) {
                        return Integer.compare(
                                o1.getCreationSequence(),
                                o2.getCreationSequence()
                        );
                    }
                }
        );

        for (DatabaseFieldDefinition table: databaseDefinition.getTables()) {
            try (Connection connection = DatabaseInitializer.getConnection(
                    databaseDefinition.getUrl(), databaseDefinition.getUser(), databaseDefinition.getPass()
                    ))
            {
                System.out.println("Creating table: " + table.getTableId());
                errorMessage = createTable(connection, configParser.initializeTableConfig(table.getPath()));

                if (!errorMessage.isEmpty())
                {
                    System.out.println("Error with table " + table.getTableLabel() + errorMessage);
                    if (table.isCritical())
                        throw new RuntimeException("Critical error while initializing " + table.getTableLabel());
                }
                else
                    System.out.println("Created table: " + table.getTableId());
            } catch (SQLException sqle)
            {
                System.out.println("Error with table " + table.getTableLabel() + sqle.getStackTrace());
                if (table.isCritical())
                    throw new RuntimeException("Critical error while initializing " + table.getTableLabel());
            }
        }
        return true;
    }

    public boolean initializeDatabase(String databaseConfigFile) {
        this.databaseConfigFile = databaseConfigFile;
        return initializeDatabase();
    }
}
