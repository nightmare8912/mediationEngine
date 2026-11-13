package org.prats.definitions.interfaces;

public interface Service {
    void start();
    void shutdown();
    void quiesce();
    void drain(long timeout);
    void cleanup();

    default String getName() {
        return this.getClass().getSimpleName();
    }
    default Service getService() { return this; }
}
