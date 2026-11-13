package org.prats.parsers.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.records.Database;
import org.prats.definitions.records.DatabaseTable;
import org.prats.loggers.FileLogger;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DatabaseConfigParser {

    public Map<String, Database> initializeDatabaseConfigs(String configFilePath) {

        FileLogger.log(configFilePath);

        Config config = ConfigFactory
                .parseFile(new File(configFilePath))
                .resolve();

        Config databases = config.getConfig("databases");

        Map<String, Database> databaseDefinitions = new LinkedHashMap<>();

        for (String databaseName : databases.root().keySet()) {

            Config databaseConfig = databases.getConfig(databaseName);

            Database database = parseDatabase(databaseConfig);

            databaseDefinitions.put(databaseName, database);
        }

        return databaseDefinitions;
    }

    private Database parseDatabase(Config database) {

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