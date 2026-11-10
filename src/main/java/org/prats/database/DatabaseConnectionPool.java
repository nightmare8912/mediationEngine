package org.prats.database;

import javax.sql.DataSource;

public interface DatabaseConnectionPool {

    void initialize(
            String url,
            String username,
            String password,
            long connectionTimeout,
            long idleTimeout,
            int maxConnections
    );

    DataSource getDataSource();

    void shutdown();
}
