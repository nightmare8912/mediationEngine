package org.prats.database.h2Database;

import com.zaxxer.hikari.HikariDataSource;
import org.prats.database.databaseUtilities.HikariDBConnectionPool.HikariDBConnectionPool;
import org.prats.definitions.Database;
import org.prats.fileHanldingUtilities.configParser.DatabaseConfigParser;

public class H2DatabaseInitializer {

    private final String databaseConfigFile;
    private final HikariDBConnectionPool dbConnectionPool;

    public H2DatabaseInitializer(String databaseConfigFile) {
        this.databaseConfigFile = databaseConfigFile;
        this.dbConnectionPool   = new HikariDBConnectionPool();
    }

    public void initializeH2Database() {

        Database database = new DatabaseConfigParser()
                .initializeDatabaseConfig(databaseConfigFile);

        dbConnectionPool.initialize(
                database.url(),
                database.user(),
                database.pass(),
                database.timeout(),
                database.idleTimeout(),
                database.maxConnections()
        );
    }

    public HikariDataSource getH2DBConnectionPool() {
        return dbConnectionPool.getDataSource();
    }
}