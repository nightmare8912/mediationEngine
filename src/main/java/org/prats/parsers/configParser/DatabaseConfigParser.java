
package org.prats.parsers.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.prats.definitions.records.Database;
import org.prats.definitions.records.DatabaseTable;
import org.prats.loggers.FileLogger;
import org.prats.utilities.ApplicationPaths;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DatabaseConfigParser {

    public Map<String, Database> initializeDatabaseConfigs(
            String configFilePath
    ) {

        Path resolvedConfigPath =
                resolveExistingFile(
                        configFilePath,
                        "Database configuration"
                );

        FileLogger.log(resolvedConfigPath.toString());

        Config config = ConfigFactory
                .parseFile(resolvedConfigPath.toFile())
                .resolve();

        Config databases = config.getConfig("databases");

        Map<String, Database> databaseDefinitions =
                new LinkedHashMap<>();

        for (String databaseName : databases.root().keySet()) {

            Config databaseConfig =
                    databases.getConfig(databaseName);

            Database database = parseDatabase(databaseConfig);

            databaseDefinitions.put(
                    databaseName,
                    database
            );
        }

        return databaseDefinitions;
    }

    private Database parseDatabase(Config database) {

        String databaseLabel = database.getString("label");
        String databaseId = database.getString("id");

        String databaseUrl = resolveDatabaseUrl(
                database.getString("url")
        );

        String databaseUser = database.getString("user");
        String databasePass = database.getString("pass");

        long databaseTimeout = database.getLong("timeout");
        long databaseIdleTimeout = database.getLong("idle_timeout");

        int databaseMaxConnections =
                database.getInt("max_connections");

        List<DatabaseTable> tables =
                database.getConfigList("tables")
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

        String tableConfigPath = resolveExistingFile(
                table.getString("path"),
                "Table configuration for " + table.getString("id")
        ).toString();

        return new DatabaseTable(
                table.getString("label"),
                table.getString("id"),
                table.getInt("creation_sequence"),
                table.getBoolean("critical"),
                tableConfigPath
        );
    }

    private String resolveDatabaseUrl(String databaseUrl) {

        if (databaseUrl == null || databaseUrl.isBlank()) {
            throw new IllegalArgumentException(
                    "Database URL cannot be empty"
            );
        }

        String prefix = "jdbc:h2:file:";

        if (!databaseUrl.startsWith(prefix)) {
            return databaseUrl;
        }

        String remaining = databaseUrl.substring(prefix.length());

        int optionsIndex = remaining.indexOf(';');

        String databasePath = optionsIndex >= 0
                ? remaining.substring(0, optionsIndex)
                : remaining;

        String options = optionsIndex >= 0
                ? remaining.substring(optionsIndex)
                : "";

        Path resolvedPath =
                ApplicationPaths.resolve(databasePath);

        return prefix + resolvedPath + options;
    }

    private Path resolveExistingFile(
            String configuredPath,
            String description
    ) {

        Path path = ApplicationPaths.resolve(configuredPath);

        if (!Files.isRegularFile(path)
                || !Files.isReadable(path)) {

            throw new IllegalArgumentException(
                    description + " does not exist or is not readable: "
                            + path
            );
        }

        return path;
    }
}
