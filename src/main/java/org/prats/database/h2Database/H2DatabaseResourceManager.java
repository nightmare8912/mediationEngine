package org.prats.database.h2Database;

import org.prats.database.databaseUtilities.DatabaseResourceManager;
import org.prats.refractoring.DatabaseConnectionPool;
import org.prats.definitions.Database;
import org.prats.fileHanldingUtilities.configParser.DatabaseConfigParser;

import javax.sql.DataSource;

public class H2DatabaseResourceManager implements DatabaseResourceManager {

    private final DatabaseConnectionPool connectionPool;
    private final String databaseConfigFile;

    public H2DatabaseResourceManager(
            String databaseConfigFile,
            DatabaseConnectionPool connectionPool
    ) {
        this.databaseConfigFile = databaseConfigFile;
        this.connectionPool = connectionPool;
    }

    @Override
    public void initialize() {

        Database database = new DatabaseConfigParser()
                .initializeDatabaseConfig(databaseConfigFile);

        connectionPool.initialize(
                database.url(),
                database.user(),
                database.pass(),
                database.timeout(),
                database.idleTimeout(),
                database.maxConnections()
        );
    }

    @Override
    public void initialize(String databaseConfigFile) {
        // If configuration is intended to be immutable,
        // consider removing this overload.
        throw new UnsupportedOperationException(
                "Database configuration is already provided"
        );
    }

    @Override
    public DataSource getDataSource() {
        return connectionPool.getDataSource();
    }

    @Override
    public void close() {
        connectionPool.shutdown();
    }
}