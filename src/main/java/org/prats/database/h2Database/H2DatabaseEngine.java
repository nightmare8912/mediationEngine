package org.prats.database.h2Database;

import com.zaxxer.hikari.HikariDataSource;
import org.prats.database.hikariDatabasePooling.HikariDataBaseUtilities;

import java.sql.Connection;
import java.sql.SQLException;

public class H2DatabaseEngine {

    private static HikariDataSource dataSource;

    public static void initialize(
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

        System.out.println("Initialized HikariH2: " + dataSource.getDataSourceProperties());
    }

    public static HikariDataSource getDataSource() {
        return dataSource;
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static void shutdown() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
