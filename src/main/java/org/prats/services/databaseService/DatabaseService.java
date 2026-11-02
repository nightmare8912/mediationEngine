package org.prats.services.databaseService;

import com.zaxxer.hikari.HikariDataSource;
import org.prats.database.h2Database.H2DatabaseEngine;
import org.prats.definitions.DatabaseDefinition;
import org.prats.internalFileHanldingUtilities.configParser.ConfigParser;
import org.prats.loggers.ShellLogger;
import org.prats.services.Service;

public class DatabaseService implements Service {
    private String databaseConfigFile;
    private static HikariDataSource dataSource;

    public DatabaseService() {}
    public DatabaseService(String databaseConfigFile) { this.databaseConfigFile = databaseConfigFile; }

    @Override
    public Service startService() {
        // use the config parser
        ShellLogger.log("");
        DatabaseDefinition databaseDefinition = new ConfigParser().initializeDatabaseConfig(databaseConfigFile);

        // initialize the connection pool and the database
        H2DatabaseEngine.initialize(
                databaseDefinition.getUrl(),
                databaseDefinition.getUser(),
                databaseDefinition.getPass(),
                databaseDefinition.getTimeout(),
                databaseDefinition.getIdleTimeout(),
                databaseDefinition.getMaxConnections()
        );

        // get the static reference of database connection pool
        dataSource = H2DatabaseEngine.getDataSource();
        return this;
    }

//    public int executeUpdate(String sql) {
//        try (
//                Connection connection = dataSource.getConnection();
//                Statement statement   = connection.createStatement();
//        ) {
//            statement.executeUpdate(sql);
//        } catch (SQLException sqlException) {
//
//        }
//    }

//    public ResultSet executeQuery(String sql) {
//
//    }

    public Service startService(String databaseConfigFile) {
        this.databaseConfigFile = databaseConfigFile;
        return startService();
    }

    @Override
    public void shutdownService() {
        H2DatabaseEngine.shutdown();
    }
}
