package org.prats.definitions;

import java.util.List;

public class Platform extends EC {
    private List<PlatformECConfig> ecConfigs;
    private String databaseConfig;

    public List<PlatformECConfig> getEcConfigs() {
        return ecConfigs;
    }

    public void setEcConfigs(List<PlatformECConfig> ecConfigs) {
        this.ecConfigs = ecConfigs;
    }

    public String getDatabaseConfig() {
        return databaseConfig;
    }

    public void setDatabaseConfig(String databaseConfig) {
        this.databaseConfig = databaseConfig;
    }
}
