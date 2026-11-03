
package org.prats.fileHanldingUtilities.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.Tab;
import org.prats.definitions.TabField;

import java.io.File;
import java.util.List;

public class TabConfigParser {

    public Tab initializeTabConfigs(String configFilePath) {

        Config config = ConfigFactory.parseFile(new File(configFilePath));

        String tabLabel = config.getString("tab.label");

        List<TabField> fields = config.getConfigList("tab.fields")
                .stream()
                .map(this::parseTabField)
                .toList();

        return new Tab(tabLabel, fields);
    }

    private TabField parseTabField(Config field) {

        return new TabField(
                field.getString("id"),
                field.getString("label"),
                field.getString("type"),
                field.getBoolean("required"),
                field.getBoolean("enabled")
        );
    }
}
