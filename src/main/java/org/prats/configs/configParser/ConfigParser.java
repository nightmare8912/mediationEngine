package org.prats.configs.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.FieldDefinition;
import org.prats.definitions.TabDefinition;
import org.prats.definitions.TableDefinition;

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
        TabDefinition         tabDefinition       = new TabDefinition();
        List<FieldDefinition> fieldDefinitionList = new ArrayList<>();

        // open, read and parse the config file
        Config config = ConfigFactory.parseFile(new File(configFilePath));

        // read the tab name and fields
        String tabLabel = config.getString("tab.label");
        var fields      = config.getConfigList("tab.fields");

        for (Config field: fields) {
            FieldDefinition fieldDefinition = new FieldDefinition();
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
        TableDefinition       tableDefinition     = new TableDefinition();
        List<FieldDefinition> fieldDefinitionList = new ArrayList<>();

        // open, read and parse the config file
        Config config = ConfigFactory.parseFile(new File(configFilePath));

        // read the tab name and fields
        String tableLabel = config.getString("table.label");
        String tableId    = config.getString("table.id");
        var fields        = config.getConfigList("table.fields");

        for (Config field: fields) {
            FieldDefinition fieldDefinition = new FieldDefinition();
            fieldDefinition.setId(field.getString("id"));
            fieldDefinition.setLabel(field.getString("label"));
            fieldDefinition.setType(field.getString("type"));
            fieldDefinition.setRequired(field.getBoolean("required"));

            fieldDefinitionList.add(fieldDefinition);
        }

        // set the fields of the tab and its name in the return value
        tableDefinition.setLabel(tableLabel);
        tableDefinition.setFields(fieldDefinitionList);
        tableDefinition.setNumberOfFields(fieldDefinitionList.size());

        return tableDefinition;
    }

    public TableDefinition initializeTableConfig(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeTableConfig();
    }
}