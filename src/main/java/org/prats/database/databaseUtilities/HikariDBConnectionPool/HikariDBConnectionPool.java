package org.prats.database.databaseUtilities.HikariDBConnectionPool;

import com.zaxxer.hikari.HikariDataSource;
import org.prats.database.hikariDatabasePooling.HikariDataBaseUtilities;
import org.prats.refractoring.DatabaseConnectionPool;

import java.sql.Connection;
import java.sql.SQLException;

public class HikariDBConnectionPool implements DatabaseConnectionPool {

    private HikariDataSource dataSource;

    public void initialize(
            String url,
            String user,
            String pass,
            long connectionTimeout,
            long idleTimeout,
            int maxPoolSize
    ) {
        // null check of datasource to avoid re-creation
        if (dataSource != null) return;

        // initializing data source
        dataSource = HikariDataBaseUtilities.initializeHikariSource(
                url,
                user,
                pass,
                connectionTimeout,
                idleTimeout,
                maxPoolSize
        );
        System.out.println("Initialized Hiakri Pool: " + dataSource.getDataSourceProperties());
    }

    public HikariDataSource getDataSource() {
        return dataSource;
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public void shutdown() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
