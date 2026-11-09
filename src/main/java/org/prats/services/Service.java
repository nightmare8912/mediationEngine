package org.prats.services;

public interface Service {
    void start();
    void shutdown();
    void quiesce();
    void drain(long timeout);

    default String getName() {
        return this.getClass().getSimpleName();
    }
    default Service getService() { return this; }
}
