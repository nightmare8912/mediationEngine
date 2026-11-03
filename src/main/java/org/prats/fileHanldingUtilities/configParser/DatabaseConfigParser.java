
package org.prats.fileHanldingUtilities.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.Database;
import org.prats.definitions.DatabaseTable;
import org.prats.loggers.ShellLogger;

import java.io.File;
import java.util.List;

public class DatabaseConfigParser {

    public Database initializeDatabaseConfig(String configFilePath) {

        ShellLogger.log(configFilePath);

        Config config = ConfigFactory.parseFile(new File(configFilePath));
        Config database = config.getConfig("database");

        String databaseLabel = database.getString("label");
        String databaseId = database.getString("id");
        String databaseUrl = database.getString("url");
        String databaseUser = database.getString("user");
        String databasePass = database.getString("pass");

        long databaseTimeout = database.getLong("timeout");
        long databaseIdleTimeout = database.getLong("idle_timeout");
        int databaseMaxConnections = database.getInt("max_connections");

        List<DatabaseTable> tables = database.getConfigList("tables")
                .stream()
                .map(this::parseDatabaseTable)
                .toList();

        return new Database(
                databaseLabel,
                databaseId,
                databaseUrl,
                databaseUser,
                databasePass,
                databaseTimeout,
                databaseIdleTimeout,
                databaseMaxConnections,
                tables
        );
    }

    private DatabaseTable parseDatabaseTable(Config table) {

        return new DatabaseTable(
                table.getString("label"),
                table.getString("id"),
                table.getInt("creation_sequence"),
                table.getBoolean("critical"),
                table.getString("path")
        );
    }
}
