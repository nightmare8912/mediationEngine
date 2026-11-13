package org.prats.utilities;

import org.prats.definitions.records.HealthReport;
import org.prats.definitions.enums.HealthStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class HealthReportAnalyzer {

    private static final long HEALTHY_THRESHOLD = 18;
    private static final long DEGRADED_THRESHOLD = 8;
    private static final long RECOVERY_FAILED_THRESHOLD = -40;

    public HealthStatus analyze(List<HealthReport> reports) {

        if (reports == null || reports.isEmpty()) {
            return HealthStatus.UNREACHABLE;
        }

        List<HealthReport> orderedReports = new ArrayList<>(reports);
        orderedReports.removeIf(report -> report == null);
        orderedReports.sort(
                Comparator.comparing(HealthReport::timestamp)
        );

        if (orderedReports.isEmpty()) {
            return HealthStatus.UNREACHABLE;
        }

        HealthReport latestReport =
                orderedReports.get(orderedReports.size() - 1);

        long ageSeconds = Duration.between(
                latestReport.timestamp(),
                Instant.now()
        ).getSeconds();

        ageSeconds = Math.max(0, ageSeconds);

        if (ageSeconds > 30 ||
                latestReport.status() == HealthStatus.UNREACHABLE) {
            return HealthStatus.UNREACHABLE;
        }

        if (latestReport.status() == HealthStatus.RECOVERY_FAILED) {
            return HealthStatus.RECOVERY_FAILED;
        }

        long totalScore = 0;
        int totalWeight = 0;

        for (int i = 0; i < orderedReports.size(); i++) {

            HealthReport currentReport = orderedReports.get(i);
            HealthReport previousReport = i > 0
                    ? orderedReports.get(i - 1)
                    : null;

            long weight = i + 1L;

            long reportScore = calculateStatusScore(
                    currentReport.status()
            );

            reportScore += calculateMetricScore(
                    currentReport.metrics(),
                    previousReport == null ? null : previousReport.metrics()
            );

            totalScore += reportScore * weight;
            totalWeight += weight;
        }

        long averageScore = Math.round(
                (double) totalScore / totalWeight
        );

        averageScore += ageSeconds <= 10 ? 10 : 0;

        return evaluateScore(averageScore);
    }

    public HealthStatus analyze(HealthReport report) {

        if (report == null) {
            return HealthStatus.UNREACHABLE;
        }

        return analyze(List.of(report));
    }

    private long calculateStatusScore(HealthStatus status) {
        return switch (status) {
            case HEALTHY                    -> 10;
            case DEGRADED                   -> 0;
            case UNHEALTHY, UNREACHABLE     -> -20;
            case RECOVERY_FAILED            -> -50;
            default                         -> 0;
        };
    }

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

        for (Map.Entry<String, Object> entry : currentMetrics.entrySet()) {

            String key = entry.getKey().toLowerCase(Locale.ROOT);

            if (!(entry.getValue() instanceof Number currentValue)) {
                continue;
            }

            long current = currentValue.longValue();

            if (key.contains("pending")) {
                if (isQuiescing) {
                    score -= Math.max(0, current) * 5;
                }
                continue;
            }

            if (previousMetrics == null) {
                continue;
            }

            Object previousValue = previousMetrics.get(entry.getKey());

            long previous = previousValue instanceof Number number
                    ? number.longValue()
                    : 0;

            long delta = Math.max(0, current - previous);

            if (delta == 0) {
                continue;
            }

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

    private HealthStatus evaluateScore(long score) {

        if (score >= HEALTHY_THRESHOLD) {
            return HealthStatus.HEALTHY;
        }

        if (score >= DEGRADED_THRESHOLD) {
            return HealthStatus.DEGRADED;
        }

        if (score > RECOVERY_FAILED_THRESHOLD) {
            return HealthStatus.UNHEALTHY;
        }

        return HealthStatus.RECOVERY_FAILED;
    }
}