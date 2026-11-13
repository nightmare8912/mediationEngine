package org.prats.definitions.interfaces;

import org.prats.definitions.records.HealthReport;

import java.util.List;

public interface HealthChecker extends HealthCheckable {
    void performHealthChecks();
    List<HealthReport> getRecentHealthReports();
    List<HealthReport> getRecentHealthReports(Service service);
}
