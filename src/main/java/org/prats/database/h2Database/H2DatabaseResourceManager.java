package org.prats.database.h2Database;

import org.prats.database.databaseUtilities.DatabaseResourceManager;
import org.prats.database.DatabaseConnectionPool;
import org.prats.definitions.Database;
import org.prats.fileHanldingUtilities.configParser.DatabaseConfigParser;

import javax.sql.DataSource;
import java.util.Objects;

public class H2DatabaseResourceManager implements DatabaseResourceManager {

    private final DatabaseConnectionPool connectionPool;
    private final String databaseConfigFile;

    private boolean initialized;

    public H2DatabaseResourceManager(
            String databaseConfigFile,
            DatabaseConnectionPool connectionPool
    ) {
        this.databaseConfigFile = Objects.requireNonNull(databaseConfigFile);
        this.connectionPool = Objects.requireNonNull(connectionPool);
    }

    @Override
    public synchronized void initialize() {

        if (initialized) {
            throw new IllegalStateException(
                    "Database resources are already initialized"
            );
        }

        try {
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

        return connectionPool.getDataSource();
    }

    @Override
    public synchronized void close() {

        try {
            connectionPool.shutdown();
        } finally {
            initialized = false;
        }
    }
}