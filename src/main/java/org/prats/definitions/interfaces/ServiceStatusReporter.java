package org.prats.definitions.interfaces;

import org.prats.definitions.records.HealthReport;

import java.util.List;

public interface ServiceStatusReporter {

    void reportServiceFailure(
            Service service,
            List<HealthReport> recentHealthReports
    );

    void reportServiceRecovered(Service service);
}
