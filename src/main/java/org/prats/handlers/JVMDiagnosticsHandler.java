package org.prats.handlers;

import org.prats.definitions.classes.Message;
import org.prats.definitions.classes.ProcessContext;
import org.prats.definitions.enums.HealthStatus;
import org.prats.definitions.enums.Operation;
import org.prats.definitions.interfaces.RequestHandler;
import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;
import org.prats.services.tcpService.TCPService;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.util.function.Supplier;

/**
 * Collects JVM diagnostics within the target process.
 *
 * Responsibilities:
 * - Collect heap and non-heap memory metrics.
 * - Collect JVM runtime information.
 * - Collect operating system metrics.
 * - Collect garbage collector statistics.
 * - Collect registry and service metrics.
 * - Collect TCP connection diagnostics.
 * - Handle explicit garbage collection requests.
 *
 * Diagnostic requests are executed inside the target JVM.
 */
public final class JVMDiagnosticsHandler implements RequestHandler {

    private static final EngineLogger log =
            EngineLogger.getLogger(JVMDiagnosticsHandler.class);

    private final MessageParser parser;

    private final Supplier<List<TCPService>> tcpServices;

    private final Supplier<HealthStatus> health;

    private Supplier<Map<String, Object>> registryMetrics =
            Map::of;

    private Supplier<Object> serviceMetrics =
            Map::of;

    public JVMDiagnosticsHandler(
            MessageParser parser,
            Supplier<List<TCPService>> tcpServices,
            Supplier<HealthStatus> health
    ) {

        this.parser = parser;
        this.tcpServices = tcpServices;
        this.health = health;

        log.debug(() ->
                "[JVMDiagnosticsHandler][constructor] JVM diagnostics handler initialized"
                        + " | parserPresent=" + (parser != null)
                        + " | tcpServicesSupplierPresent=" + (tcpServices != null)
                        + " | healthSupplierPresent=" + (health != null)
        );
    }

    /**
     * Configures the supplier used to retrieve service metrics.
     */
    public void setServiceMetrics(
            Supplier<Object> metrics
    ) {

        log.debug(() ->
                "[JVMDiagnosticsHandler][setServiceMetrics] Configuring service metrics supplier"
                        + " | supplierPresent=" + (metrics != null)
        );

        this.serviceMetrics = metrics;
    }

    /**
     * Configures the supplier used to retrieve registry metrics.
     */
    public void setRegistryMetrics(
            Supplier<Map<String, Object>> metrics
    ) {

        log.debug(() ->
                "[JVMDiagnosticsHandler][setRegistryMetrics] Configuring registry metrics supplier"
                        + " | supplierPresent=" + (metrics != null)
        );

        this.registryMetrics = metrics;
    }

    /**
     * Converts JVM memory usage information into a structured map.
     *
     * All memory values are expressed in bytes.
     */
    private Map<String, Long> memory(
            MemoryUsage usage
    ) {

        return Map.of(
                "used", usage.getUsed(),
                "committed", usage.getCommitted(),
                "max", usage.getMax(),
                "initial", usage.getInit()
        );
    }

    /**
     * Handles incoming JVM diagnostic requests.
     *
     * Returns a complete diagnostic snapshot containing
     * JVM, operating system, registry, service, and
     * connection information.
     */
    @Override
    public Message handleRequest(Message request) {

        boolean gc =
                request.getOperation() == Operation.RUN_GC;

        log.debug(() ->
                "[JVMDiagnosticsHandler][handleRequest] JVM diagnostic request received"
                        + " | correlationId=" + request.getCorrelationId()
                        + " | operation=" + request.getOperation()
                        + " | gcRequested=" + gc
                        + " | processName=" + ProcessContext.getProcessName()
        );

        try {

            /*
             * Request explicit garbage collection if required.
             *
             * The JVM may ignore explicit GC requests
             * depending on its configuration.
             */
            if (gc) {

                log.info(() ->
                        "[JVMDiagnosticsHandler][handleRequest] Explicit garbage collection requested"
                                + " | correlationId=" + request.getCorrelationId()
                );

                ManagementFactory.getMemoryMXBean().gc();

                log.debug(() ->
                        "[JVMDiagnosticsHandler][handleRequest] Explicit GC invocation returned"
                                + " | correlationId=" + request.getCorrelationId()
                );
            }

            /*
             * Acquire JVM management interfaces.
             */
            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] Initializing JVM management beans"
                            + " | correlationId=" + request.getCorrelationId()
            );

            var runtime =
                    ManagementFactory.getRuntimeMXBean();

            var memoryBean =
                    ManagementFactory.getMemoryMXBean();

            var operatingSystem =
                    ManagementFactory.getOperatingSystemMXBean();

            Map<String, Object> snapshot =
                    new LinkedHashMap<>();

            /*
             * Collect JVM identity and runtime information.
             */
            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] Collecting JVM runtime information"
                            + " | correlationId=" + request.getCorrelationId()
            );

            snapshot.put(
                    "id",
                    ProcessContext.getProcessName()
            );

            snapshot.put(
                    "pid",
                    ProcessHandle.current().pid()
            );

            snapshot.put(
                    "connected",
                    true
            );

            HealthStatus healthStatus = health.get();

            snapshot.put(
                    "status",
                    healthStatus.name()
            );

            snapshot.put(
                    "uptimeMillis",
                    runtime.getUptime()
            );

            snapshot.put(
                    "javaVersion",
                    System.getProperty("java.version")
            );

            snapshot.put(
                    "jvmName",
                    runtime.getVmName()
            );

            snapshot.put(
                    "jvmArguments",
                    runtime.getInputArguments()
            );

            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] JVM runtime information collected"
                            + " | correlationId=" + request.getCorrelationId()
                            + " | healthStatus=" + healthStatus
                            + " | uptimeMillis=" + runtime.getUptime()
            );

            /*
             * Collect operating system information.
             */
            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] Collecting operating system metrics"
                            + " | correlationId=" + request.getCorrelationId()
            );

            Map<String, Object> machine =
                    new LinkedHashMap<>();

            machine.put(
                    "processors",
                    operatingSystem.getAvailableProcessors()
            );

            machine.put(
                    "loadAverage",
                    operatingSystem.getSystemLoadAverage()
            );

            machine.put(
                    "os",
                    operatingSystem.getName()
            );

            /*
             * Collect extended operating system metrics
             * when supported by the current JVM.
             */
            if (operatingSystem instanceof
                    com.sun.management.OperatingSystemMXBean system) {

                machine.put(
                        "cpuLoad",
                        system.getCpuLoad()
                );

                machine.put(
                        "processCpuLoad",
                        system.getProcessCpuLoad()
                );

                machine.put(
                        "memoryTotal",
                        system.getTotalMemorySize()
                );

                machine.put(
                        "memoryFree",
                        system.getFreeMemorySize()
                );

                log.debug(() ->
                        "[JVMDiagnosticsHandler][handleRequest] Extended operating system metrics collected"
                                + " | correlationId=" + request.getCorrelationId()
                );
            }

            snapshot.put(
                    "machine",
                    machine
            );

            /*
             * Collect registry metrics.
             */
            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] Collecting registry metrics"
                            + " | correlationId=" + request.getCorrelationId()
            );

            snapshot.put(
                    "registries",
                    registryMetrics.get()
            );

            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] Registry metrics collected"
                            + " | correlationId=" + request.getCorrelationId()
            );

            /*
             * Collect service metrics.
             */
            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] Collecting service metrics"
                            + " | correlationId=" + request.getCorrelationId()
            );

            snapshot.put(
                    "services",
                    serviceMetrics.get()
            );

            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] Service metrics collected"
                            + " | correlationId=" + request.getCorrelationId()
            );

            /*
             * Collect heap and non-heap memory information.
             */
            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] Collecting JVM memory metrics"
                            + " | correlationId=" + request.getCorrelationId()
            );

            MemoryUsage heapUsage =
                    memoryBean.getHeapMemoryUsage();

            MemoryUsage nonHeapUsage =
                    memoryBean.getNonHeapMemoryUsage();

            snapshot.put(
                    "heap",
                    memory(heapUsage)
            );

            snapshot.put(
                    "nonHeap",
                    memory(nonHeapUsage)
            );

            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] JVM memory metrics collected"
                            + " | correlationId=" + request.getCorrelationId()
                            + " | heapUsed=" + heapUsage.getUsed()
                            + " | heapCommitted=" + heapUsage.getCommitted()
                            + " | heapMax=" + heapUsage.getMax()
                            + " | nonHeapUsed=" + nonHeapUsage.getUsed()
            );

            /*
             * Collect individual JVM memory pool information.
             */
            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] Collecting JVM memory pool metrics"
                            + " | correlationId=" + request.getCorrelationId()
            );

            var memoryPools =
                    ManagementFactory.getMemoryPoolMXBeans()
                            .stream()
                            .map(pool -> {

                                Map<String, Object> row =
                                        new LinkedHashMap<>();

                                row.put(
                                        "name",
                                        pool.getName()
                                );

                                MemoryUsage usage = pool.getUsage();

                                if (usage != null) {

                                    row.putAll(
                                            memory(usage)
                                    );
                                }

                                return row;

                            })
                            .toList();

            snapshot.put(
                    "memoryPools",
                    memoryPools
            );

            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] JVM memory pool metrics collected"
                            + " | correlationId=" + request.getCorrelationId()
                            + " | memoryPoolCount=" + memoryPools.size()
            );

            /*
             * Collect garbage collector metrics.
             */
            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] Collecting garbage collector metrics"
                            + " | correlationId=" + request.getCorrelationId()
            );

            var garbageCollectors =
                    ManagementFactory.getGarbageCollectorMXBeans()
                            .stream()
                            .map(collector ->
                                    Map.of(
                                            "name", collector.getName(),
                                            "collections", collector.getCollectionCount(),
                                            "timeMillis", collector.getCollectionTime()
                                    )
                            )
                            .toList();

            snapshot.put(
                    "gc",
                    garbageCollectors
            );

            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] Garbage collector metrics collected"
                            + " | correlationId=" + request.getCorrelationId()
                            + " | collectorCount=" + garbageCollectors.size()
            );

            /*
             * Collect TCP connection diagnostics.
             */
            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] Collecting TCP connection diagnostics"
                            + " | correlationId=" + request.getCorrelationId()
            );

            List<TCPService> services =
                    tcpServices.get();

            var connections =
                    services.stream()
                            .flatMap(service ->
                                    service.connectionDetails().stream()
                            )
                            .toList();

            snapshot.put(
                    "connections",
                    connections
            );

            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] TCP connection diagnostics collected"
                            + " | correlationId=" + request.getCorrelationId()
                            + " | tcpServiceCount=" + services.size()
                            + " | connectionCount=" + connections.size()
            );

            /*
             * Add diagnostic operation information.
             */
            snapshot.put(
                    "gcRequested",
                    gc
            );

            snapshot.put(
                    "body",
                    gc
                            ? "GC requested. The JVM may ignore explicit GC requests."
                            : "Live JVM diagnostics."
            );

            /*
             * Construct the diagnostic response.
             */
            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] Creating JVM diagnostic response"
                            + " | correlationId=" + request.getCorrelationId()
                            + " | snapshotFields=" + snapshot.size()
            );

            Message response = parser.createResponse(
                    request.getCorrelationId(),
                    Map.of()
            );

            response.setPayload(
                    parser.convertToJsonNode(snapshot)
            );

            response.setResponseComplete(true);

            log.debug(() ->
                    "[JVMDiagnosticsHandler][handleRequest] JVM diagnostic response created"
                            + " | correlationId=" + request.getCorrelationId()
                            + " | responseComplete=true"
            );

            return response;

        } catch (RuntimeException exception) {

            /*
             * Avoid logging diagnostic payloads or JVM arguments,
             * which may contain sensitive information.
             */
            log.error(
                    "[JVMDiagnosticsHandler][handleRequest] JVM diagnostic request failed"
                            + " | correlationId=" + request.getCorrelationId()
                            + " | operation=" + request.getOperation()
                            + " | exceptionType="
                            + exception.getClass().getSimpleName()
            );

            throw exception;
        }
    }
}