package org.prats.utilities;

import org.junit.jupiter.api.Test;
import org.prats.definitions.enums.HealthStatus;
import org.prats.definitions.records.HealthReport;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class HealthReportAnalyzerTest {
    private final HealthReportAnalyzer analyzer = new HealthReportAnalyzer();
    private HealthReport report(String name, HealthStatus status, long failures) {
        return new HealthReport(name, status, "test", Instant.now(), Map.of("failedTasks", failures));
    }

    @Test
    void unrelatedServiceCountersCannotCreateRecoveryFailure() {
        List<HealthReport> services = List.of(
                report("health", HealthStatus.HEALTHY, 0),
                report("message", HealthStatus.HEALTHY, 1369),
                report("application-tcp", HealthStatus.HEALTHY, 14113),
                report("control-tcp", HealthStatus.HEALTHY, 7));
        assertEquals(HealthStatus.HEALTHY, analyzer.aggregateServices(services));
        assertTrue(new HealthReportFormatter().format(services, "ec01", null)
                .contains("Overall status: HEALTHY"));
    }

    @Test
    void unhealthyServiceIsNotHiddenByHealthyPeers() {
        for (HealthStatus status : List.of(HealthStatus.DEGRADED, HealthStatus.UNHEALTHY,
                HealthStatus.UNREACHABLE, HealthStatus.RECOVERY_FAILED, HealthStatus.RECOVERING)) {
            assertEquals(status, analyzer.aggregateServices(List.of(
                    report("bad", status, 0), report("good", HealthStatus.HEALTHY, 10000))));
        }
    }

    @Test
    void staleAndMissingReportsAreUnreachable() {
        assertEquals(HealthStatus.UNREACHABLE, analyzer.aggregateServices(List.of()));
        assertEquals(HealthStatus.UNREACHABLE, analyzer.aggregateServices(List.of(
                new HealthReport("old", HealthStatus.HEALTHY, "", Instant.now().minusSeconds(60), Map.of()))));
    }

    @Test
    void failureScoreDoesNotClaimRecoveryWasAttempted() {
        HealthReport earlier = new HealthReport("message", HealthStatus.HEALTHY, "",
                Instant.now().minusSeconds(1), Map.of("failedTasks", 0));
        assertEquals(HealthStatus.UNHEALTHY, analyzer.analyze(List.of(earlier,
                report("message", HealthStatus.UNHEALTHY, 1000))));
        assertEquals(HealthStatus.RECOVERY_FAILED,
                analyzer.analyze(report("message", HealthStatus.RECOVERY_FAILED, 0)));
    }
}
