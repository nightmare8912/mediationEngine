package org.prats.database.h2Database;

import org.prats.database.databaseUtilities.DatabaseResourceManager;
import org.prats.database.DatabaseConnectionPool;
import org.prats.definitions.records.Database;
import org.prats.parsers.configParser.DatabaseConfigParser;

import javax.sql.DataSource;
import java.util.Objects;

public class H2DatabaseResourceManager implements DatabaseResourceManager {

    private final DatabaseConnectionPool connectionPool;
    private final String databaseConfigFile;

    private volatile boolean initialized;

    public H2DatabaseResourceManager(
            String databaseConfigFile,
            DatabaseConnectionPool connectionPool
    ) {
        this.databaseConfigFile     = Objects.requireNonNull(databaseConfigFile);
        this.connectionPool         = Objects.requireNonNull(connectionPool);
        this.initialized            = false;
    }


    @Override
    public synchronized void initialize() {

        if (initialized) {
            throw new IllegalStateException(
                    "Database resources are already initialized"
            );
        }

        try {
            Database database = Objects.requireNonNull(
                    new DatabaseConfigParser()
                            .initializeDatabaseConfigs(databaseConfigFile)
                            .get("internal-h2"),
                    "Internal H2 database configuration was not found"
            );
            connectionPool.initialize(
                    database.url(),
                    database.user(),
                    database.pass(),
                    database.timeout(),
                    database.idleTimeout(),
                    database.maxConnections()
            );
            Objects.requireNonNull(
                    connectionPool.getDataSource(),
                    "DataSource initialization failed"
            );
            initialized = true;
        } catch (RuntimeException exception) {
            try {
                connectionPool.shutdown();
            } catch (RuntimeException cleanupException) {
                exception.addSuppressed(cleanupException);
            }
            initialized = false;
            throw exception;
        }
    }



    @Override
    public synchronized DataSource getDataSource() {
        if (!initialized) {
            throw new IllegalStateException(
                    "Database resources have not been initialized"
            );
        }
        return Objects.requireNonNull(
                connectionPool.getDataSource(),
                "Initialized database DataSource is unavailable"
        );
    }



    @Override
    public synchronized void close() {
        connectionPool.shutdown();
        initialized = false;
    }

}