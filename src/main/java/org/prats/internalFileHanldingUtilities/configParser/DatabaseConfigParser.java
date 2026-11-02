package org.prats.internalFileHanldingUtilities.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.Database;
import org.prats.definitions.DatabaseTable;
import org.prats.loggers.ShellLogger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DatabaseConfigParser {
    private String configFilePath;

    public DatabaseConfigParser () {}

    public DatabaseConfigParser (String configFilePath) {
        this.configFilePath = configFilePath;
    }

    public Database initializeDatabaseConfig() {
        // declare the variables
        Database database = new Database();
        List<DatabaseTable> fieldDefinitionList = new ArrayList<>();

        // open, read and parse the config file
        ShellLogger.log(configFilePath);
        Config config = ConfigFactory.parseFile(new File(configFilePath));

        // read the tab name and fields
        String databaseLabel        = config.getString("database.label");
        String databaseId           = config.getString("database.id");
        String databaseUrl          = config.getString("database.url");
        String databaseUser         = config.getString("database.user");
        String databasePass         = config.getString("database.pass");
        long databaseTimeout        = config.getLong("database.timeout");
        long databaseIdleTimeout    = config.getLong("database.idle_timeout");
        int databaseMaxConnections  = config.getInt("database.max_connections");
        var tables                  = config.getConfigList("database.tables");

        for (Config table: tables)
        {
            DatabaseTable fieldDefinition = new DatabaseTable();
            fieldDefinition.setTableLabel(table.getString("label"));
            fieldDefinition.setTableId(table.getString("id"));
            fieldDefinition.setCreationSequence(table.getInt("creation_sequence"));
            fieldDefinition.setCritical(table.getBoolean("critical"));
            fieldDefinition.setPath(table.getString("path"));

            fieldDefinitionList.add(fieldDefinition);
        }

        // set the fields of the tab and its name in the return value
        database.setLabel(databaseLabel);
        database.setId(databaseId);
        database.setUrl(databaseUrl);
        database.setUser(databaseUser);
        database.setPass(databasePass);
        database.setTimeout(databaseTimeout);
        database.setIdleTimeout(databaseIdleTimeout);
        database.setMaxConnections(databaseMaxConnections);
        database.setTables(fieldDefinitionList);

        return database;
    }

    public Database initializeDatabaseConfig(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeDatabaseConfig();
    }
}
