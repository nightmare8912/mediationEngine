import org.prats.configs.configParser.ConfigParser;
import org.prats.definitions.FieldDefinition;
import org.prats.definitions.TabDefinition;
import org.prats.definitions.TableDefinition;

public class Test {
    public static void main(String[] args) {
        ConfigParser configParser = new ConfigParser();
//        TabDefinition tabDefinition = configParser.initializeTabConfigs(
//                "/Users/pratyush./Desktop/coding/mediationEngine2/src/main/java/org/prats/configs/tabConfigs/general_disk_tab.conf"
//        );

        TableDefinition tableDefinition = configParser.initializeTableConfig(
                "/Users/pratyush./Desktop/coding/mediationEngine2/src/main/java/org/prats/configs/tableConfigs/batch_agents_table.conf"
        );

        System.out.println(tableDefinition.getLabel());

        for (FieldDefinition field: tableDefinition.getFields()) {
            System.out.println(field.getId());
        }

        // while (true) {}
    }
}
