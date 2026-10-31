package configs.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ConfigParser {

    private String configFilePath;

    public ConfigParser () {}

    public ConfigParser (String configFilePath) {
        this.configFilePath = configFilePath;
    }

    public void initalizeConfigs() {

        Config config = ConfigFactory.parseFile(new File(configFilePath));

        String tabName = config.getString("tab.name");
        var fields     = config.getConfigList("tab.fields");

        System.out.println("Tab Name: " + tabName);

        for (Config field: fields)
        {
            String id       = field.getString("id");
            String label    = field.getString("label");
            String type     = field.getString("type");

            System.out.println(
                    id   + " | " + label + " | " +
                    type + " | " + required
            );
        }
    }

    public void initalizeConfigs(String configFilePath) {
        this.configFilePath = configFilePath;
        initalizeConfigs();
    }
}