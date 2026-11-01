package org.prats.database.h2Database;

import org.prats.internalFileHanldingUtilities.configParser.ConfigParser;
import org.prats.database.databaseUtilities.SQLSyntaxHelper;
import org.prats.definitions.DatabaseDefinition;
import org.prats.definitions.DatabaseTableDefinition;
import org.prats.definitions.TableDefinition;
import org.prats.definitions.TableFieldDefinition;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Comparator;

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


    private StringBuilder createTable(Connection connection, TableDefinition tableDefinition) throws SQLException {

        SQLSyntaxHelper sqlSyntaxHelper = new SQLSyntaxHelper();
        StringBuilder errorMessage      = validateTable(tableDefinition);

        if (errorMessage.isEmpty())
        {
            String sql = sqlSyntaxHelper.createTableStatementHelper(tableDefinition);
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

    public void initializeDatabase() {
        StringBuilder errorMessage;
        ConfigParser configParser = new ConfigParser();
        DatabaseDefinition databaseDefinition = configParser.initializeDatabaseConfig(databaseConfigFile);

        databaseDefinition.getTables().sort(
                new Comparator<DatabaseTableDefinition>() {
                    @Override
                    public int compare(DatabaseTableDefinition o1, DatabaseTableDefinition o2) {
                        return Integer.compare(
                                o1.getCreationSequence(),
                                o2.getCreationSequence()
                        );
                    }
                }
        );

        H2DatabaseEngine.initialize(
                databaseDefinition.getUrl(),
                databaseDefinition.getUser(),
                databaseDefinition.getPass(),
                databaseDefinition.getTimeout(),
                databaseDefinition.getIdleTimeout(),
                databaseDefinition.getMaxConnections()
        );

        for (DatabaseTableDefinition table: databaseDefinition.getTables()) {
            try (Connection connection = H2DatabaseEngine.getConnection())
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
                System.out.println("Error with table " + table.getTableLabel() + Arrays.toString(sqle.getStackTrace()));
                if (table.isCritical())
                    throw new RuntimeException("Critical error while initializing " + table.getTableLabel());
            }
        }
    }

    public void initializeDatabase(String databaseConfigFile) {
        this.databaseConfigFile = databaseConfigFile;
        initializeDatabase();
    }
}
