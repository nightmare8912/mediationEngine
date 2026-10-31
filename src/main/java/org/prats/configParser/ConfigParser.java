package org.prats.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.TabFieldDefinition;
import org.prats.definitions.TabDefinition;
import org.prats.definitions.TableDefinition;
import org.prats.definitions.TableFieldDefinition;
import org.prats.definitions.DatabaseDefinition;
import org.prats.definitions.DatabaseFieldDefinition;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ConfigParser {

    private String configFilePath;

    public ConfigParser () {}

    public ConfigParser (String configFilePath) {
        this.configFilePath = configFilePath;
    }

    public TabDefinition initializeTabConfigs() {
        // declare the variables
        TabDefinition            tabDefinition       = new TabDefinition();
        List<TabFieldDefinition> fieldDefinitionList = new ArrayList<>();

        // open, read and parse the config file
        Config config = ConfigFactory.parseFile(new File(configFilePath));

        // read the tab name and fields
        String tabLabel = config.getString("tab.label");
        var fields      = config.getConfigList("tab.fields");

        for (Config field: fields)
        {
            TabFieldDefinition fieldDefinition = new TabFieldDefinition();
            fieldDefinition.setId(field.getString("id"));
            fieldDefinition.setLabel(field.getString("label"));
            fieldDefinition.setType(field.getString("type"));
            fieldDefinition.setRequired(field.getBoolean("required"));
            fieldDefinition.setEnabled(field.getBoolean("enabled"));

            fieldDefinitionList.add(fieldDefinition);
        }

        // set the fields of the tab and its name in the return value
        tabDefinition.setLabel(tabLabel);
        tabDefinition.setFields(fieldDefinitionList);

        return tabDefinition;
    }

    public TabDefinition initializeTabConfigs(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeTabConfigs();
    }

    public void initializeAgentConfig() {
        //TODO: pending
    }

    public void initializeAgentConfig(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        initializeAgentConfig();
    }

    public TableDefinition initializeTableConfig() {
        // declare the variables
        TableDefinition            tableDefinition     = new TableDefinition();
        List<TableFieldDefinition> fieldDefinitionList = new ArrayList<>();

        // open, read and parse the config file
        Config config = ConfigFactory.parseFile(new File(configFilePath));

        // read the tab name and fields
        String tableLabel = config.getString("table.label");
        String tableId    = config.getString("table.id");
        var fields        = config.getConfigList("table.fields");

        for (Config field: fields)
        {
            TableFieldDefinition fieldDefinition = new TableFieldDefinition();
            fieldDefinition.setId(field.getString("id"));
            fieldDefinition.setLabel(field.getString("label"));
            fieldDefinition.setType(field.getString("type"));
            fieldDefinition.setRequired(field.getBoolean("required"));

            if (field.hasPath(("primary_key")))
                fieldDefinition.setPrimaryKey(field.getBoolean("primary_key"));

            fieldDefinitionList.add(fieldDefinition);
        }

        // set the fields of the tab and its name in the return value
        tableDefinition.setLabel(tableLabel);
        tableDefinition.setId(tableId);
        tableDefinition.setFields(fieldDefinitionList);
        tableDefinition.setNumberOfFields(fieldDefinitionList.size());

        return tableDefinition;
    }

    public TableDefinition initializeTableConfig(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeTableConfig();
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
        databaseDefinition.setTables(fieldDefinitionList);

        return databaseDefinition;
    }

    public DatabaseDefinition initializeDatabaseConfig(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeDatabaseConfig();
    }
}