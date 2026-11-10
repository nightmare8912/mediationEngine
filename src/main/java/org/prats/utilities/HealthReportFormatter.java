package org.prats.utilities;

import org.prats.definitions.HealthReport;

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

    public String format(List<HealthReport> reports) {

        if (reports == null || reports.isEmpty()) {
            return "No health reports available.";
        }

        Map<String, HealthReport> latestReports = new TreeMap<>();

        for (HealthReport report : reports) {
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

        appendCentered(output, "PLATFORM HEALTH REPORT");
        output.append(BORDER).append(NL);

        long healthy = latestReports.values().stream()
                .filter(report -> "HEALTHY".equals(
                        String.valueOf(report.status())))
                .count();

        int total = latestReports.size();
        int unhealthy = total - (int) healthy;

        output.append(String.format(
                "Services: %d    Healthy: %d    Attention required: %d%n%n",
                total, healthy, unhealthy
        ));

        // Service overview
        output.append("SERVICE OVERVIEW").append(NL);
        output.append(DIVIDER).append(NL);

        output.append(String.format(
                "%-25s %-18s %s%n",
                "SERVICE", "STATUS", "DESCRIPTION"
        ));

        output.append(DIVIDER).append(NL);

        for (HealthReport report : latestReports.values()) {
            output.append(String.format(
                    "%-25s %-18s %s%n",
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
                            formatMetricValue(value)
                    ));
                }
            }
        }

        output.append(NL).append(BORDER);

        return output.toString();
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

        String formatted = metric.replaceAll(
                "([a-z0-9])([A-Z])", "$1 $2"
        );

        return Character.toUpperCase(formatted.charAt(0))
                + formatted.substring(1);
    }

    private String formatMetricValue(Object value) {

        if (value instanceof Boolean booleanValue) {
            return booleanValue ? "Yes" : "No";
        }

        return String.valueOf(value);
    }
}
