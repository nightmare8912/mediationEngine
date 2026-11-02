package org.prats.internalFileHanldingUtilities.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.Table;
import org.prats.definitions.TableField;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TableConfigParser {
    private String configFilePath;

    public TableConfigParser () {}

    public TableConfigParser (String configFilePath) {
        this.configFilePath = configFilePath;
    }

    public Table initializeTableConfig() {
        // declare the variables
        Table table = new Table();
        List<TableField> fieldDefinitionList = new ArrayList<>();

        // open, read and parse the config file
        Config config = ConfigFactory.parseFile(new File(configFilePath));

        // read the tab name and fields
        String tableLabel = config.getString("table.label");
        String tableId    = config.getString("table.id");
        var fields        = config.getConfigList("table.fields");

        for (Config field: fields)
        {
            TableField fieldDefinition = new TableField();
            fieldDefinition.setId(field.getString("id"));
            fieldDefinition.setLabel(field.getString("label"));
            fieldDefinition.setType(field.getString("type"));
            fieldDefinition.setRequired(field.getBoolean("required"));

            if (field.hasPath(("primary_key")))
                fieldDefinition.setPrimaryKey(field.getBoolean("primary_key"));

            fieldDefinitionList.add(fieldDefinition);
        }

        // set the fields of the tab and its name in the return value
        table.setLabel(tableLabel);
        table.setId(tableId);
        table.setFields(fieldDefinitionList);
        table.setNumberOfFields(fieldDefinitionList.size());

        return table;
    }

    public Table initializeTableConfig(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeTableConfig();
    }
}
