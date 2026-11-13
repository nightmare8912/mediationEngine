package org.prats.parsers.configParser;

import org.prats.definitions.records.*;

import java.util.Map;

public class ConfigParser {

    private String configFilePath;

    public ConfigParser () {}

    public ConfigParser (String configFilePath) {
        this.configFilePath = configFilePath;
    }

    public Tab initializeTabConfigs() {
        return new TabConfigParser().initializeTabConfigs(configFilePath);
    }

    public Tab initializeTabConfigs(String configFilePath) {
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

    public Table initializeTableConfig() {
        return new TableConfigParser().initializeTableConfig(configFilePath);
    }

    public Table initializeTableConfig(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeTableConfig();
    }

    public Map<String, Database> initializeDatabaseConfig() {
       return new DatabaseConfigParser().initializeDatabaseConfigs(configFilePath);
    }

    public Map<String, Database> initializeDatabaseConfig(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeDatabaseConfig();
    }

    public FilePath initializeFilePaths() {
        return new FilePathParser().initializeFilePaths(configFilePath);
    }

    public FilePath initializeFilePaths(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeFilePaths();
    }

    public EC initializeECConfigs() {
        return new ECConfigParser().initializeECConfigs(configFilePath);
    }

    public EC initializeECConfigs(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeECConfigs();
    }

    public Platform initializePlatformConfigs() {
        return new PlatformConfigParser().initializePlatformConfigs(configFilePath);
    }

    public Platform initializePlatformConfigs(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializePlatformConfigs();
    }
}