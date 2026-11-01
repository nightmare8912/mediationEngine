package org.prats.definitions;

import java.util.List;

public class PlatformDefinition extends ECDefinition {
    private List<PlatformECConfigDefinition> ecConfigs;

    public List<PlatformECConfigDefinition> getEcConfigs() {
        return ecConfigs;
    }

    public void setEcConfigs(List<PlatformECConfigDefinition> ecConfigs) {
        this.ecConfigs = ecConfigs;
    }
}
