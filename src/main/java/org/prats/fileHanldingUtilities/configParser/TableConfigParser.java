
package org.prats.fileHanldingUtilities.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.Table;
import org.prats.definitions.TableField;

import java.io.File;
import java.util.List;

public class TableConfigParser {

    public Table initializeTableConfig(String configFilePath) {

        Config config = ConfigFactory.parseFile(new File(configFilePath));

        String tableLabel = config.getString("table.label");
        String tableId = config.getString("table.id");

        List<TableField> fields = config.getConfigList("table.fields")
                .stream()
                .map(this::parseTableField)
                .toList();

        return new Table(tableLabel, tableId, fields);
    }

    private TableField parseTableField(Config field) {

        return new TableField(
                field.getString("id"),
                field.getString("label"),
                field.getString("type"),
                field.getBoolean("required"),
                field.hasPath("primary_key") && field.getBoolean("primary_key")
        );
    }
}
