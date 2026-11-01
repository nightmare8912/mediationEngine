package org.prats.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.DatabaseDefinition;
import org.prats.definitions.DatabaseFieldDefinition;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DatabaseConfigParser {
    private String configFilePath;

    public DatabaseConfigParser () {}

    public DatabaseConfigParser (String configFilePath) {
        this.configFilePath = configFilePath;
    }

    public DatabaseDefinition initializeDatabaseConfig() {
        // declare the variables
        DatabaseDefinition            databaseDefinition  = new DatabaseDefinition();
        List<DatabaseFieldDefinition> fieldDefinitionList = new ArrayList<>();

        // open, read and parse the config file
        Config config = ConfigFactory.parseFile(new File(configFilePath));

        // read the tab name and fields
        String databaseLabel = config.getString("database.label");
        String databaseId    = config.getString("database.id");
        String databaseUrl   = config.getString("database.url");
        String databaseUser  = config.getString("database.user");
        String databasePass  = config.getString("database.pass");
        long databaseTimeout = config.getLong("database.timeout");
        long databaseIdleTimeout = config.getLong("database.idle_timeout");
        int databaseMaxConnections = config.getInt("database.max_connections");
        var tables           = config.getConfigList("database.tables");

        for (Config table: tables)
        {
            DatabaseFieldDefinition fieldDefinition = new DatabaseFieldDefinition();
            fieldDefinition.setTableLabel(table.getString("label"));
            fieldDefinition.setTableId(table.getString("id"));
            fieldDefinition.setCreationSequence(table.getInt("creation_sequence"));
            fieldDefinition.setCritical(table.getBoolean("critical"));
            fieldDefinition.setPath(table.getString("path"));

            fieldDefinitionList.add(fieldDefinition);
        }

        // set the fields of the tab and its name in the return value
        databaseDefinition.setLabel(databaseLabel);
        databaseDefinition.setId(databaseId);
        databaseDefinition.setUrl(databaseUrl);
        databaseDefinition.setUser(databaseUser);
        databaseDefinition.setPass(databasePass);
        databaseDefinition.setTimeout(databaseTimeout);
        databaseDefinition.setIdleTimeout(databaseIdleTimeout);
        databaseDefinition.setMaxConnections(databaseMaxConnections);
        databaseDefinition.setTables(fieldDefinitionList);

        return databaseDefinition;
    }

    public DatabaseDefinition initializeDatabaseConfig(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeDatabaseConfig();
    }
}
