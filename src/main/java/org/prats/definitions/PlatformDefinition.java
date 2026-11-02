package org.prats.definitions;

import java.util.List;

public class PlatformDefinition extends ECDefinition {
    private List<PlatformECConfigDefinition> ecConfigs;
    private String databaseConfig;

    public List<PlatformECConfigDefinition> getEcConfigs() {
        return ecConfigs;
    }

    public void setEcConfigs(List<PlatformECConfigDefinition> ecConfigs) {
        this.ecConfigs = ecConfigs;
    }

    public String getDatabaseConfig() {
        return databaseConfig;
    }

    public void setDatabaseConfig(String databaseConfig) {
        this.databaseConfig = databaseConfig;
    }
}
