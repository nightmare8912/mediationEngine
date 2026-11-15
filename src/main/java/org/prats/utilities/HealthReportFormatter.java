package org.prats.utilities;

import org.prats.definitions.records.HealthReport;
import org.prats.definitions.enums.HealthStatus;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class HealthReportFormatter {

    private static final int WIDTH = 90;
    private static final String NL = System.lineSeparator();

    private static final String BORDER = "=".repeat(WIDTH);
    private static final String DIVIDER = "-".repeat(WIDTH);

    private static final DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")
                    .withZone(ZoneOffset.UTC);

    public String format(
            List<HealthReport> reports,
            String containerName,
            Instant containerStartedAt
    ) {

        Map<String, HealthReport> latestReports = new TreeMap<>();

        for (HealthReport report : reports == null ? List.<HealthReport>of() : reports) {
            latestReports.merge(
                    report.serviceName(),
                    report,
                    (existing, candidate) ->
                            candidate.timestamp().isAfter(existing.timestamp())
                                    ? candidate
                                    : existing
            );
        }

        StringBuilder output = new StringBuilder();
        Instant generatedAt = Instant.now();

        appendCentered(output, "CONTAINER HEALTH REPORT");
        output.append(String.format("Container : %s%n", safe(containerName)));
        output.append(String.format("Started   : %s%n",
                containerStartedAt == null ? "Unknown" : TIME_FORMAT.format(containerStartedAt)));
        output.append(String.format("Uptime    : %s%n",
                containerStartedAt == null ? "Unknown" : formatUptime(Math.max(0,
                        Duration.between(containerStartedAt, generatedAt).getSeconds()))));
        output.append(String.format("Generated : %s%n", TIME_FORMAT.format(generatedAt)));
        output.append(BORDER).append(NL);

        long healthy = countStatus(latestReports, HealthStatus.HEALTHY);
        long degraded = countStatus(latestReports, HealthStatus.DEGRADED);
        long unhealthy = countStatus(latestReports, HealthStatus.UNHEALTHY);
        long unreachable = countStatus(latestReports, HealthStatus.UNREACHABLE);
        long recoveryFailed = countStatus(latestReports, HealthStatus.RECOVERY_FAILED);
        HealthStatus overallStatus = new HealthReportAnalyzer().aggregateServices(
                new ArrayList<>(latestReports.values()));

        int total = latestReports.size();
        output.append(String.format(
                "Overall status: %s%nServices: %d    Healthy: %d    Degraded: %d    Unhealthy: %d    Unreachable: %d    Recovery failed: %d%n%n",
                overallStatus, total, healthy, degraded, unhealthy, unreachable, recoveryFailed
        ));

        // Service overview
        output.append("SERVICE OVERVIEW").append(NL);
        output.append(DIVIDER).append(NL);

        output.append(String.format(
                "%-28s %-16s %s%n",
                "SERVICE", "STATUS", "DESCRIPTION"
        ));

        output.append(DIVIDER).append(NL);

        if (latestReports.isEmpty()) {
            output.append("No service health checks available.").append(NL);
        }

        for (HealthReport report : latestReports.values()) {
            output.append(String.format(
                    "%-28s %-16s %s%n",
                    report.serviceName(),
                    report.status(),
                    report.message()
            ));
        }

        output.append(NL).append(BORDER).append(NL);

        // Detailed reports
        output.append("DETAILED HEALTH INFORMATION").append(NL);

        for (HealthReport report : latestReports.values()) {

            output.append(NL).append(DIVIDER).append(NL);

            output.append(String.format(
                    "[%s] %s%n",
                    report.status(),
                    report.serviceName()
            ));

            output.append(String.format(
                    "    Last check : %s%n",
                    TIME_FORMAT.format(report.timestamp())
            ));

            output.append(String.format(
                    "    Message    : %s%n",
                    report.message()
            ));

            if (report.metrics() != null
                    && !report.metrics().isEmpty()) {

                output.append(NL)
                        .append("    METRICS").append(NL);

                List<String> metricNames =
                        new ArrayList<>(report.metrics().keySet());

                Collections.sort(metricNames);

                for (String metric : metricNames) {

                    Object value = report.metrics().get(metric);

                    output.append(String.format(
                            "        %-38s : %s%n",
                            formatMetricName(metric),
                            formatMetricValue(metric, value)
                    ));
                }
            }
        }

        output.append(NL).append(BORDER);

        return output.toString();
    }

    private String safe(String value) {
        return value == null || value.isBlank() ? "Unknown" : value;
    }

    private long countStatus(
            Map<String, HealthReport> reports,
            HealthStatus status
    ) {
        return reports.values().stream()
                .filter(report -> report.status() == status)
                .count();
    }

    private void appendCentered(
            StringBuilder output,
            String title
    ) {
        int padding = Math.max(0, (WIDTH - title.length()) / 2);

        output.append(BORDER).append(NL)
                .append(" ".repeat(padding))
                .append(title)
                .append(NL)
                .append(BORDER)
                .append(NL).append(NL);
    }

    private String formatMetricName(String metric) {

        if (metric.equals("healthServiceUptimeSeconds")) {
            return "Health Service Uptime";
        }

        String formatted = metric.replaceAll(
                "([a-z0-9])([A-Z])",
                "$1 $2"
        );

        return Character.toUpperCase(formatted.charAt(0))
                + formatted.substring(1);
    }

    private String formatMetricValue(
            String metric,
            Object value
    ) {

        if (value instanceof Boolean booleanValue) {
            return booleanValue ? "Yes" : "No";
        }

        if ((metric.equals("healthServiceUptimeSeconds")
                || metric.equals("containerUptimeSeconds"))
                && value instanceof Number number) {

            return formatUptime(number.longValue());
        }

        return String.valueOf(value);
    }

    private String formatUptime(long totalSeconds) {

        long days = totalSeconds / 86400;
        long hours = (totalSeconds % 86400) / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;

        return String.format(
                Locale.ROOT,
                "%d days, %02d hours, %02d minutes, %02d seconds",
                days,
                hours,
                minutes,
                seconds
        );
    }
}
