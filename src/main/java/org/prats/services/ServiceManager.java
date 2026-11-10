package org.prats.services;

import org.prats.definitions.HealthReport;

import java.util.List;

public interface ServiceManager extends Service {
    void start(Service service);
    void quiesce(Service service);
    void drain(Service service);
    void shutdown(Service service);
    void restart(Service service);
    void restartAll();
    List<HealthReport> getHealthReports();
    List<HealthReport> getHealthReports(Service service);
}
