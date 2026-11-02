package org.prats.database.h2Database;

import org.prats.internalFileHanldingUtilities.configParser.ConfigParser;
import org.prats.database.databaseUtilities.sqlHelper.SQLSyntaxHelper;
import org.prats.definitions.Database;
import org.prats.definitions.DatabaseTable;
import org.prats.definitions.Table;
import org.prats.definitions.TableField;

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

    private StringBuilder validateTable(Table table) {
        boolean isError = false;
        StringBuilder errorMessage = new StringBuilder();

        if (table.getLabel() == null || table.getId().isEmpty())
        {
            errorMessage.append("Table ID is empty in file ");
            isError = true;
        }

        if (!isError && table.getNumberOfFields() == 0)
        {
            errorMessage.append("No fields in the table");
            isError = true;
        }

        if (!isError)
        {
            for (TableField tableField : table.getFields())
            {
                if (tableField.getId() == null || tableField.getId().isEmpty())
                {
                    errorMessage.append("Field name empty in table ");
                    isError = true; break;
                }
                if (tableField.getType() == null || tableField.getType().equals(""))
                {
                    errorMessage.append("Field type empty for field "); errorMessage.append(tableField.getId());
                    isError = true; break;
                }
            }
        }

        return errorMessage;
    }


    private StringBuilder createTable(Connection connection, Table table) throws SQLException {

        SQLSyntaxHelper sqlSyntaxHelper = new SQLSyntaxHelper();
        StringBuilder errorMessage      = validateTable(table);

        if (errorMessage.isEmpty())
        {
            String sql = sqlSyntaxHelper.createTableStatementHelper(table);
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
        Database database = configParser.initializeDatabaseConfig(databaseConfigFile);

        database.getTables().sort(
                new Comparator<DatabaseTable>() {
                    @Override
                    public int compare(DatabaseTable o1, DatabaseTable o2) {
                        return Integer.compare(
                                o1.getCreationSequence(),
                                o2.getCreationSequence()
                        );
                    }
                }
        );

        H2DatabaseEngine.initialize(
                database.getUrl(),
                database.getUser(),
                database.getPass(),
                database.getTimeout(),
                database.getIdleTimeout(),
                database.getMaxConnections()
        );

        for (DatabaseTable table: database.getTables()) {
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
