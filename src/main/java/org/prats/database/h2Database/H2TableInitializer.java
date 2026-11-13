package org.prats.database.h2Database;

import org.prats.database.databaseUtilities.sqlHelper.SQLQuery;
import org.prats.parsers.configParser.ConfigParser;
import org.prats.database.databaseUtilities.sqlHelper.SQLSyntaxHelper;
import org.prats.definitions.records.Database;
import org.prats.definitions.records.DatabaseTable;
import org.prats.definitions.records.Table;
import org.prats.definitions.records.TableField;
import org.prats.database.DatabaseConnectionPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Comparator;

public class H2TableInitializer {
    private String databaseConfigFile;
    private DatabaseConnectionPool databaseConnectionPool;

    public H2TableInitializer(
            String databaseConfigFile,
            DatabaseConnectionPool databaseConnectionPool
    ) {
        this.databaseConfigFile     = databaseConfigFile;
        this.databaseConnectionPool = databaseConnectionPool;
    }


    private StringBuilder validateTable(Table table) {
        boolean isError = false;
        StringBuilder errorMessage = new StringBuilder();

        if (table.label() == null || table.id().isEmpty())
        {
            errorMessage.append("Table ID is empty in file ");
            isError = true;
        }

        if (!isError && table.numberOfFields() == 0)
        {
            errorMessage.append("No fields in the table");
            isError = true;
        }

        if (!isError)
        {
            for (TableField tableField : table.fields())
            {
                if (tableField.id() == null || tableField.id().isEmpty())
                {
                    errorMessage.append("Field name empty in table ");
                    isError = true; break;
                }
                if (tableField.type() == null || tableField.type().isEmpty())
                {
                    errorMessage.append("Field type empty for field "); errorMessage.append(tableField.id());
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
            SQLQuery sql = sqlSyntaxHelper.createTableStatementHelper(table);
            System.out.println(sql.query());
            try (Statement statement = connection.createStatement())
            {
                statement.execute(sql.query());
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
        Database database = configParser.initializeDatabaseConfig(databaseConfigFile).get("internal-h2");

        database.tables().sort(
                new Comparator<DatabaseTable>() {
                    @Override
                    public int compare(DatabaseTable o1, DatabaseTable o2) {
                        return Integer.compare(
                                o1.creationSequence(),
                                o2.creationSequence()
                        );
                    }
                }
        );

        databaseConnectionPool.initialize(
                database.url(),
                database.user(),
                database.pass(),
                database.timeout(),
                database.idleTimeout(),
                database.maxConnections()
        );

        for (DatabaseTable table: database.tables()) {
            try (Connection connection = databaseConnectionPool.getDataSource().getConnection())
            {
                System.out.println("Creating table: " + table.tableId());
                errorMessage = createTable(connection, configParser.initializeTableConfig(table.path()));

                if (!errorMessage.isEmpty())
                {
                    System.out.println("Error with table " + table.tableLabel() + errorMessage);
                    if (table.critical())
                        throw new RuntimeException("Critical error while initializing " + table.tableLabel());
                }
                else
                    System.out.println("Created table: " + table.tableId());
            } catch (SQLException sqle)
            {
                System.out.println("Error with table " + table.tableLabel() + Arrays.toString(sqle.getStackTrace()));
                if (table.critical())
                    throw new RuntimeException("Critical error while initializing " + table.tableLabel());
            }
        }
    }

    public void initializeDatabase(String databaseConfigFile) {
        this.databaseConfigFile = databaseConfigFile;
        initializeDatabase();
    }
}