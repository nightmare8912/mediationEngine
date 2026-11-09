package org.prats.services;

import org.prats.definitions.HealthReport;

import java.util.List;

public interface ServiceFailureReporter {

    void reportServiceFailure(
            Service service,
            List<HealthReport> recentHealthReports
    );

    void reportServiceRecovered(Service service);
}
