package org.prats.services.healthCheckService;

import org.prats.definitions.HealthCheckable;
import org.prats.definitions.HealthReport;
import org.prats.services.Service;

import java.util.List;

public interface HealthChecker extends HealthCheckable {
    void performHealthChecks();
    List<HealthReport> getRecentHealthReports();
    List<HealthReport> getRecentHealthReports(Service service);
}
