package org.prats.database.databaseUtilities.HikariDBConnectionPool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.prats.database.DatabaseConnectionPool;

import javax.sql.DataSource;
import java.util.Objects;

public class HikariDBConnectionPool implements DatabaseConnectionPool {

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

        Objects.requireNonNull(url, "Database URL cannot be null");
        Objects.requireNonNull(user, "Database user cannot be null");
        Objects.requireNonNull(pass, "Database password cannot be null");

        HikariDataSource existing = dataSource;

        if (existing != null && !existing.isClosed()) {

            throw new IllegalStateException(
                    "Connection pool is already initialized"
            );
        }

        if (existing != null) {
            dataSource = null;
        }

        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(pass);

        config.setConnectionTimeout(timeout);
        config.setIdleTimeout(idleTimeout);
        config.setMaximumPoolSize(maxConnections);

        config.setPoolName("MediationEngine-H2-Pool");
        HikariDataSource newDataSource =
                new HikariDataSource(config);
        dataSource = newDataSource;
    }



    @Override
    public synchronized DataSource getDataSource() {
        HikariDataSource current = dataSource;
        if (current == null || current.isClosed()) {
            throw new IllegalStateException(
                    "Hikari connection pool is not initialized"
            );
        }
        return current;
    }



    @Override
    public synchronized void shutdown() {
        HikariDataSource current = dataSource;
        if (current == null) {
            return;
        }
        if (!current.isClosed()) {
            current.close();
        }
        dataSource = null;
    }
}