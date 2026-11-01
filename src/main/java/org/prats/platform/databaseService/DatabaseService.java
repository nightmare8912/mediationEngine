package org.prats.platform.databaseService;

import com.zaxxer.hikari.HikariDataSource;
import org.prats.database.h2Database.H2DatabaseEngine;
import org.prats.definitions.DatabaseDefinition;
import org.prats.internalFileHanldingUtilities.configParser.ConfigParser;
import org.prats.platform.PlatformInterface;

public class DatabaseService implements PlatformInterface {
    private String databaseConfigFile;
    private static HikariDataSource dataSource;

    public DatabaseService() {}
    public DatabaseService(String databaseConfigFile) {}

    @Override
    public void startService() {
        // use the config parser
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

    }

    @Override
    public void shutdownService() {
        H2DatabaseEngine.shutdown();
    }
}
