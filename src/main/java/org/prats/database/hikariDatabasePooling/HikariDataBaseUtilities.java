package org.prats.database.hikariDatabasePooling;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class HikariDataBaseUtilities {

    public static HikariDataSource initializeHikariSource(
            String url,
            String user,
            String pass,
            long connectionTimout,
            long idleTimeout,
            int maxPoolSize
    ) {

        HikariConfig config = new HikariConfig();

        // database connection param
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(pass);

        // connection pool params
        config.setConnectionTimeout(connectionTimout);
        config.setIdleTimeout(idleTimeout);
        config.setMaximumPoolSize(maxPoolSize);

        // initializing data source
        return new HikariDataSource(config);
    }
}
