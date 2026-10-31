package org.prats.configParser;

public class AgentConfigParser {
    private String configFilePath;

    public AgentConfigParser () {}

    public AgentConfigParser (String configFilePath) {
        this.configFilePath = configFilePath;
    }

    public void initializeAgentConfig() {
        //TODO: pending
    }

    public void initializeAgentConfig(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        initializeAgentConfig();
    }
}
