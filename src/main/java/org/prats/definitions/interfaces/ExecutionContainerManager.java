package org.prats.definitions.interfaces;

import org.prats.container.ContainerProcess;
import org.prats.definitions.records.ConsolidatedECConfig;

public interface ExecutionContainerManager {
    void notifyMissingHeartbeat(ConsolidatedECConfig ecConfig);

    void startEC(String ecId);
    void stopEC(String ecId);
    void restartEC(String ecId);
}
