package org.prats.database.databaseUtilities.HikariDBConnectionPool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.prats.database.DatabaseConnectionPool;
import org.prats.loggers.EngineLogger;

import javax.sql.DataSource;
import java.util.Objects;

public class HikariDBConnectionPool implements DatabaseConnectionPool {

    private static final EngineLogger log =
            EngineLogger.getLogger(HikariDBConnectionPool.class);

    private volatile HikariDataSource dataSource;


    @Override
    public synchronized void initialize(
            String url,
            String user,
            String pass,
            long timeout,
            long idleTimeout,
            int maxConnections
    ) {

        log.debug(() ->
                "[HikariDBConnectionPool][initialize] Connection pool initialization requested"
                        + " | timeout=" + timeout
                        + " | idleTimeout=" + idleTimeout
                        + " | maxConnections=" + maxConnections
        );

        Objects.requireNonNull(url, "Database URL cannot be null");
        Objects.requireNonNull(user, "Database user cannot be null");
        Objects.requireNonNull(pass, "Database password cannot be null");

        HikariDataSource existing = dataSource;

        if (existing != null && !existing.isClosed()) {

            log.warn(
                    "[HikariDBConnectionPool][initialize] Initialization rejected because connection pool is already active"
            );

            throw new IllegalStateException(
                    "Connection pool is already initialized"
            );
        }

        if (existing != null) {
            dataSource = null;

            log.debug(
                    "[HikariDBConnectionPool][initialize] Cleared reference to previously closed connection pool"
            );
        }

        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(pass);

        config.setConnectionTimeout(timeout);
        config.setIdleTimeout(idleTimeout);
        config.setMaximumPoolSize(maxConnections);

        config.setPoolName("MediationEngine-H2-Pool");

        log.debug(() ->
                "[HikariDBConnectionPool][initialize] Hikari configuration prepared"
                        + " | poolName=MediationEngine-H2-Pool"
                        + " | connectionTimeout=" + timeout
                        + " | idleTimeout=" + idleTimeout
                        + " | maximumPoolSize=" + maxConnections
        );

        HikariDataSource newDataSource =
                new HikariDataSource(config);

        dataSource = newDataSource;

        log.info(() ->
                "[HikariDBConnectionPool][initialize] Hikari connection pool initialized successfully"
                        + " | poolName=MediationEngine-H2-Pool"
                        + " | maximumPoolSize=" + maxConnections
                        + " | closed=" + newDataSource.isClosed()
        );
    }



    @Override
    public synchronized DataSource getDataSource() {

        HikariDataSource current = dataSource;

        if (current == null || current.isClosed()) {

            log.warn(() ->
                    "[HikariDBConnectionPool][getDataSource] Datasource unavailable"
                            + " | datasourceInitialized=" + (current != null)
                            + " | datasourceClosed=" + (current != null && current.isClosed())
            );

            throw new IllegalStateException(
                    "Hikari connection pool is not initialized"
            );
        }

        log.debug(() ->
                "[HikariDBConnectionPool][getDataSource] Active datasource retrieved"
                        + " | poolName=" + current.getPoolName()
                        + " | closed=" + current.isClosed()
        );

        return current;
    }



    @Override
    public synchronized void shutdown() {

        log.debug(
                "[HikariDBConnectionPool][shutdown] Connection pool shutdown requested"
        );

        HikariDataSource current = dataSource;

        if (current == null) {

            log.debug(
                    "[HikariDBConnectionPool][shutdown] No datasource initialized; nothing to shut down"
            );

            return;
        }

        if (!current.isClosed()) {

            log.info(() ->
                    "[HikariDBConnectionPool][shutdown] Closing Hikari connection pool"
                            + " | poolName=" + current.getPoolName()
            );

            current.close();

        } else {

            log.debug(
                    "[HikariDBConnectionPool][shutdown] Hikari connection pool was already closed"
            );
        }

        dataSource = null;

        log.info(
                "[HikariDBConnectionPool][shutdown] Hikari connection pool shutdown completed"
        );
    }
}