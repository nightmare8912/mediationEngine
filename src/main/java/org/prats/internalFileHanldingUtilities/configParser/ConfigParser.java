package org.prats.internalFileHanldingUtilities.configParser;

import org.prats.definitions.*;

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

    public FilePathDefinition initializeFilePaths() {
        return new FilePathParser().initializeFilePaths(configFilePath);
    }

    public FilePathDefinition initializeFilePaths(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeFilePaths();
    }

    public ECDefinition initializeECConfigs() {
        return new ECConfigParser().initializeECConfigs(configFilePath);
    }

    public ECDefinition initializeECConfigs(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeECConfigs();
    }

    public PlatformDefinition initializePlatformConfigs() {
        return new PlatformConfigParser().initializePlatformConfigs(configFilePath);
    }

    public PlatformDefinition initializePlatformConfigs(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializePlatformConfigs();
    }
}