
package org.prats.utilities;

import org.prats.definitions.enums.HealthStatus;
import org.prats.definitions.records.HealthReport;

import java.time.Duration;
import java.time.Instant;

import java.util.*;

/**
 * Analyzes service health reports using status severity,
 * historical metrics, and weighted health scoring.
 */
public class HealthReportAnalyzer {

    private static final long HEALTHY_THRESHOLD = 18;
    private static final long DEGRADED_THRESHOLD = 8;


    // =========================================================
    // 1. AGGREGATE INDEPENDENT SERVICE HEALTH REPORTS
    // =========================================================

    /**
     * Combines independent services without comparing
     * their unrelated counters.
     */
    public HealthStatus aggregateServices(
            List<HealthReport> reports
    ) {

        if (reports == null || reports.isEmpty()) {
            return HealthStatus.UNREACHABLE;
        }

        HealthStatus overall = HealthStatus.HEALTHY;
        Instant now = Instant.now();

        for (HealthReport report : reports) {

            HealthStatus status =
                    report == null || report.status() == null
                            ? HealthStatus.UNKNOWN
                            : report.status();

            if (report != null
                    && (report.timestamp() == null
                    || Duration.between(
                    report.timestamp(),
                    now
            ).getSeconds() > 30)) {

                status = HealthStatus.UNREACHABLE;
            }

            if (severity(status) > severity(overall)) {
                overall = status;
            }
        }

        return overall;
    }


    // =========================================================
    // 2. HEALTH STATUS SEVERITY
    // =========================================================

    private int severity(HealthStatus status) {

        return switch (status) {

            case HEALTHY -> 0;

            case DEGRADED -> 1;

            case RECOVERING -> 2;

            case UNKNOWN, INVALID -> 3;

            case UNHEALTHY -> 4;

            case UNREACHABLE -> 5;

            case RECOVERY_FAILED -> 6;
        };
    }


    // =========================================================
    // 3. ANALYZE HISTORICAL HEALTH REPORTS
    // =========================================================

    public HealthStatus analyze(
            List<HealthReport> reports
    ) {

        if (reports == null || reports.isEmpty()) {
            return HealthStatus.UNREACHABLE;
        }


        // -----------------------------------------------------
        // 3.1. Sort reports chronologically
        // -----------------------------------------------------

        List<HealthReport> orderedReports =
                new ArrayList<>(reports);

        orderedReports.removeIf(
                Objects::isNull
        );

        orderedReports.sort(
                Comparator.comparing(HealthReport::timestamp)
        );

        if (orderedReports.isEmpty()) {
            return HealthStatus.UNREACHABLE;
        }


        // -----------------------------------------------------
        // 3.2. Identify the latest health report
        // -----------------------------------------------------

        HealthReport latestReport =
                orderedReports.getLast(
                );

        long ageSeconds = Duration.between(
                latestReport.timestamp(),
                Instant.now()
        ).getSeconds();

        ageSeconds = Math.max(0, ageSeconds);


        // -----------------------------------------------------
        // 3.3. Check report freshness and critical states
        // -----------------------------------------------------

        if (ageSeconds > 30
                || latestReport.status() == HealthStatus.UNREACHABLE) {

            return HealthStatus.UNREACHABLE;
        }

        if (latestReport.status() == HealthStatus.RECOVERY_FAILED) {
            return HealthStatus.RECOVERY_FAILED;
        }


        // -----------------------------------------------------
        // 3.4. Calculate weighted historical health score
        // -----------------------------------------------------

        long totalScore = 0;
        int totalWeight = 0;

        for (int i = 0; i < orderedReports.size(); i++) {

            HealthReport currentReport =
                    orderedReports.get(i);

            HealthReport previousReport =
                    i > 0
                            ? orderedReports.get(i - 1)
                            : null;

            long weight = i + 1L;

            long reportScore = calculateStatusScore(
                    currentReport.status()
            );

            reportScore += calculateMetricScore(
                    currentReport.metrics(),

                    previousReport == null
                            ? null
                            : previousReport.metrics()
            );

            totalScore += reportScore * weight;

            totalWeight += weight;
        }


        // -----------------------------------------------------
        // 3.5. Calculate average weighted score
        // -----------------------------------------------------

        long averageScore = Math.round(
                (double) totalScore / totalWeight
        );


        // -----------------------------------------------------
        // 3.6. Reward fresh health reports
        // -----------------------------------------------------

        averageScore += ageSeconds <= 10
                ? 10
                : 0;


        // -----------------------------------------------------
        // 3.7. Evaluate final health status
        // -----------------------------------------------------

        return evaluateScore(averageScore);
    }


    // =========================================================
    // 4. ANALYZE A SINGLE HEALTH REPORT
    // =========================================================

    public HealthStatus analyze(
            HealthReport report
    ) {

        if (report == null) {
            return HealthStatus.UNREACHABLE;
        }

        return analyze(
                List.of(report)
        );
    }


    // =========================================================
    // 5. CALCULATE STATUS SCORE
    // =========================================================

    private long calculateStatusScore(
            HealthStatus status
    ) {

        return switch (status) {

            case HEALTHY -> 10;

            case DEGRADED -> 0;

            case UNHEALTHY, UNREACHABLE -> -20;

            case RECOVERY_FAILED -> -50;

            default -> 0;
        };
    }


    // =========================================================
    // 6. CALCULATE METRIC SCORE
    // =========================================================

    private long calculateMetricScore(
            Map<String, Object> currentMetrics,
            Map<String, Object> previousMetrics
    ) {

        if (currentMetrics == null) {
            return 0;
        }

        long score = 0;

        boolean isQuiescing = Boolean.TRUE.equals(
                currentMetrics.get("isQuiescing")
        );


        // -----------------------------------------------------
        // 6.1. Iterate through available metrics
        // -----------------------------------------------------

        for (Map.Entry<String, Object> entry
                : currentMetrics.entrySet()) {

            String key = entry.getKey()
                    .toLowerCase(Locale.ROOT);

            if (!(entry.getValue() instanceof Number currentValue)) {
                continue;
            }

            long current = currentValue.longValue();


            // -------------------------------------------------
            // 6.2. Evaluate pending work during quiescing
            // -------------------------------------------------

            if (key.contains("pending")) {

                if (isQuiescing) {

                    score -= Math.max(0, current) * 5;
                }

                continue;
            }


            // -------------------------------------------------
            // 6.3. Skip delta comparison without history
            // -------------------------------------------------

            if (previousMetrics == null) {
                continue;
            }

            Object previousValue =
                    previousMetrics.get(entry.getKey());

            long previous =
                    previousValue instanceof Number number
                            ? number.longValue()
                            : 0;

            long delta = Math.max(
                    0,
                    current - previous
            );

            if (delta == 0) {
                continue;
            }


            // -------------------------------------------------
            // 6.4. Evaluate metric changes
            // -------------------------------------------------

            if (key.contains("quiesce")) {

                score -= delta;

            } else if (key.contains("reject")) {

                score -= delta * 8;

            } else if (key.contains("fail")) {

                score -= delta * 4;

            } else if (key.contains("complete")) {

                score += delta;
            }
        }

        return score;
    }


    // =========================================================
    // 7. EVALUATE FINAL HEALTH SCORE
    // =========================================================

    private HealthStatus evaluateScore(
            long score
    ) {

        if (score >= HEALTHY_THRESHOLD) {
            return HealthStatus.HEALTHY;
        }

        if (score >= DEGRADED_THRESHOLD) {
            return HealthStatus.DEGRADED;
        }

        // A poor score indicates ill health,
        // not a failed recovery attempt.

        return HealthStatus.UNHEALTHY;
    }
}
