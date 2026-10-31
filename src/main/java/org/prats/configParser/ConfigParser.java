package org.prats.configParser;

import org.prats.definitions.TabDefinition;
import org.prats.definitions.TableDefinition;
import org.prats.definitions.DatabaseDefinition;

public class ConfigParser {

    private String configFilePath;

    public ConfigParser () {}

    public ConfigParser (String configFilePath) {
        this.configFilePath = configFilePath;
    }

    public TabDefinition initializeTabConfigs() {
        return new TabConfigParser().initializeTabConfigs(configFilePath);
    }

    public TabDefinition initializeTabConfigs(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeTabConfigs();
    }

    public void initializeAgentConfig() {
        // TODO: this is pending
        // return new AgentConfigParser().initializeAgentConfig(configFilePath);
    }

    public void initializeAgentConfig(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        initializeAgentConfig();
    }

    public TableDefinition initializeTableConfig() {
        return new TableConfigParser().initializeTableConfig(configFilePath);
    }

    public TableDefinition initializeTableConfig(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeTableConfig();
    }

    public DatabaseDefinition initializeDatabaseConfig() {
       return new DatabaseConfigParser().initializeDatabaseConfig(configFilePath);
    }

    public DatabaseDefinition initializeDatabaseConfig(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeDatabaseConfig();
    }
}