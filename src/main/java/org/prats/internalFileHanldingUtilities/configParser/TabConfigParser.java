package org.prats.internalFileHanldingUtilities.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.TabDefinition;
import org.prats.definitions.TabFieldDefinition;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TabConfigParser {
    private String configFilePath;

    public TabConfigParser () {}

    public TabConfigParser (String configFilePath) {
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
}
