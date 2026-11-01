package org.prats.internalFileHanldingUtilities.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.TableDefinition;
import org.prats.definitions.TableFieldDefinition;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TableConfigParser {
    private String configFilePath;

    public TableConfigParser () {}

    public TableConfigParser (String configFilePath) {
        this.configFilePath = configFilePath;
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
}
