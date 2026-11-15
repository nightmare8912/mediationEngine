
package org.prats.container.initializers;

import org.prats.database.databaseUtilities.HikariDBConnectionPool.HikariDBConnectionPool;
import org.prats.database.h2Database.H2DatabaseResourceManager;

import org.prats.definitions.classes.ProcessContext;
import org.prats.definitions.enums.ProcessType;
import org.prats.definitions.enums.TCPConnectionMode;
import org.prats.definitions.interfaces.ServiceRegistry;
import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.definitions.records.ConsolidatedWatchdogConfig;
import org.prats.definitions.records.Platform;
import org.prats.definitions.records.TCPPeer;
import org.prats.definitions.records.Watchdog;

import org.prats.forwarders.CommandForwarder;
import org.prats.forwarders.DatabaseForwarder;
import org.prats.forwarders.GUIForwarder;

import org.prats.handlers.DatabaseRequestHandler;
import org.prats.handlers.PlatformCommandHandler;
import org.prats.handlers.PlatformControlHandler;

import org.prats.loggers.EngineLogger;

import org.prats.parsers.CommandParser;
import org.prats.parsers.MessageParser;
import org.prats.parsers.configParser.ConfigParser;
import org.prats.parsers.configParser.WatchdogConfigParser;

import org.prats.processors.CommandProcessor;
import org.prats.processors.MessageProcessor;
import org.prats.processors.RequestProcessor;
import org.prats.processors.ResponseProcessor;
import org.prats.processors.TCPApplicationProcessor;
import org.prats.processors.TCPProtocolProcessor;

import org.prats.registries.ContainerServiceRegistry;
import org.prats.registries.CorrelationRegistry;
import org.prats.registries.HeartbeatRegistry;
import org.prats.registries.RetryRegistry;

import org.prats.services.ExternalHealthMonitor;
import org.prats.services.containerServices.PlatformService;
import org.prats.services.databaseService.DatabaseService;
import org.prats.services.healthCheckService.HealthService;
import org.prats.services.messageService.MessageService;
import org.prats.services.tcpService.TCPService;

import org.prats.utilities.ApplicationPaths;
import org.prats.utilities.ContainerRuntime;
import org.prats.utilities.HealthReportAnalyzer;

import java.nio.file.Files;
import java.nio.file.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Entry point and composition root for the PLATFORM JVM.
 *
 * Responsibilities:
 *
 * 1. Parse JVM startup arguments.
 * 2. Load and validate the PLATFORM configuration.
 * 3. Initialize the process context.
 * 4. Create registries, parsers, handlers, and services.
 * 5. Connect dependencies between services.
 * 6. Register services with the service registry.
 * 7. Start the PLATFORM lifecycle.
 *
 * This class creates and wires infrastructure.
 * Runtime business logic belongs to the individual services.
 */
public class PlatformInitializer {

    private static final EngineLogger log =
            EngineLogger.getLogger(PlatformInitializer.class);

    private static final Set<String> SUPPORTED_ARGUMENTS = Set.of(
            "tcp-port",
            "tcp-backlog",
            "control-port",
            "core-threads",
            "max-threads",
            "idle-timeout",
            "database-conf",
            "container-name",
            "container-type",
            "container-conf"
    );

    private static long HEART_BEAT_TTL = 45L;

    private Platform platformDefinition;

    private Path platformConfigPath;
    private Path databaseConfigPath;

    private ServiceRegistry serviceRegistry;

    private PlatformService platformService;
    private MessageService messageService;
    private DatabaseService databaseService;

    private boolean initializationAttempted;


    // =========================================================
    // 1. JVM ARGUMENT PARSING
    // =========================================================

    /**
     * Parses supported JVM application arguments.
     *
     * Accepted formats:
     *
     * --key=value
     * --key value
     *
     * Arguments must be recognized and cannot be duplicated.
     */
    private Map<String, String> parseArguments(String[] args) {

        log.debug(() ->
                "[PlatformInitializer][parseArguments] "
                        + "Beginning argument parsing"
                        + " | argumentCount="
                        + (args == null ? "null" : args.length)
        );

        if (args == null) {

            log.warn(
                    "[PlatformInitializer][parseArguments] "
                            + "Argument array is null"
            );

            throw new IllegalArgumentException(
                    "Platform arguments cannot be null"
            );
        }

        Map<String, String> parameters = new LinkedHashMap<>();

        for (int i = 0; i < args.length; i++) {

            String argument = args[i];

            // -------------------------------------------------
            // 1.1. Validate argument format
            // -------------------------------------------------

            if (argument == null || !argument.startsWith("--")) {

                log.warn(() ->
                        "[PlatformInitializer][parseArguments] "
                                + "Invalid argument format"
                                + " | argument=" + argument
                );

                throw new IllegalArgumentException(
                        "Invalid argument: " + argument
                );
            }

            int separator = argument.indexOf('=');

            String key;
            String value;

            // -------------------------------------------------
            // 1.2. Extract argument key and value
            // -------------------------------------------------

            if (separator >= 0) {

                key = argument.substring(2, separator);
                value = argument.substring(separator + 1);

            } else {

                key = argument.substring(2);

                if (i + 1 >= args.length
                        || args[i + 1].startsWith("--")) {

                    log.warn(() ->
                            "[PlatformInitializer][parseArguments] "
                                    + "Missing argument value"
                                    + " | argument=" + key
                    );

                    throw new IllegalArgumentException(
                            "Missing value for argument: --" + key
                    );
                }

                value = args[++i];
            }

            // -------------------------------------------------
            // 1.3. Validate supported arguments
            // -------------------------------------------------

            if (!SUPPORTED_ARGUMENTS.contains(key)) {

                log.warn(() ->
                        "[PlatformInitializer][parseArguments] "
                                + "Unsupported argument"
                                + " | argument=" + key
                );

                throw new IllegalArgumentException(
                        "Unknown platform argument: --" + key
                );
            }

            // -------------------------------------------------
            // 1.4. Validate argument value
            // -------------------------------------------------

            if (value == null || value.isBlank()) {

                log.warn(() ->
                        "[PlatformInitializer][parseArguments] "
                                + "Empty argument value"
                                + " | argument=" + key
                );

                throw new IllegalArgumentException(
                        "Empty value supplied for argument: --" + key
                );
            }

            // -------------------------------------------------
            // 1.5. Reject duplicate arguments
            // -------------------------------------------------

            if (parameters.putIfAbsent(key, value) != null) {

                log.warn(() ->
                        "[PlatformInitializer][parseArguments] "
                                + "Duplicate argument"
                                + " | argument=" + key
                );

                throw new IllegalArgumentException(
                        "Duplicate platform argument: --" + key
                );
            }

            log.debug(() ->
                    "[PlatformInitializer][parseArguments] "
                            + "Argument accepted"
                            + " | key=" + key
            );
        }

        log.debug(() ->
                "[PlatformInitializer][parseArguments] "
                        + "Argument parsing completed"
                        + " | parsedKeys=" + parameters.keySet()
        );

        return parameters;
    }


    // =========================================================
    // 2. REQUIRED ARGUMENT RESOLUTION
    // =========================================================

    private String required(
            Map<String, String> parameters,
            String key
    ) {

        String value = parameters.get(key);

        if (value == null || value.isBlank()) {

            log.warn(() ->
                    "[PlatformInitializer][required] "
                            + "Required argument missing"
                            + " | argument=" + key
            );

            throw new IllegalArgumentException(
                    "Missing required argument: --" + key
            );
        }

        log.debug(() ->
                "[PlatformInitializer][required] "
                        + "Required argument resolved"
                        + " | argument=" + key
        );

        return value.trim();
    }


    // =========================================================
    // 3. CONFIGURATION PATH RESOLUTION
    // =========================================================

    /**
     * Resolves the PLATFORM configuration path.
     */
    private Path resolveConfigPath(String configuredPath) {

        log.debug(() ->
                "[PlatformInitializer][resolveConfigPath] "
                        + "Resolving PLATFORM configuration"
                        + " | configuredPath=" + configuredPath
        );

        Path path = ApplicationPaths.resolve(configuredPath);

        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {

            log.warn(() ->
                    "[PlatformInitializer][resolveConfigPath] "
                            + "Configuration file is missing or unreadable"
                            + " | path=" + path
            );

            throw new IllegalArgumentException(
                    "Configuration file does not exist or is not readable: "
                            + path
            );
        }

        log.debug(() ->
                "[PlatformInitializer][resolveConfigPath] "
                        + "Configuration path resolved"
                        + " | path=" + path
        );

        return path;
    }


    /**
     * Resolves the database configuration path.
     *
     * Relative paths are resolved through ApplicationPaths.
     */
    private Path resolveDatabaseConfigPath(String configuredPath) {

        Objects.requireNonNull(
                configuredPath,
                "Database configuration path cannot be null"
        );

        log.debug(() ->
                "[PlatformInitializer][resolveDatabaseConfigPath] "
                        + "Resolving database configuration"
                        + " | configuredPath=" + configuredPath
        );

        Path path = Path.of(configuredPath);

        if (!path.isAbsolute()) {
            path = ApplicationPaths.resolve(configuredPath);
        }

        Path resolvedPath = path.toAbsolutePath().normalize();

        if (!Files.isRegularFile(resolvedPath)
                || !Files.isReadable(resolvedPath)) {

            log.warn(() ->
                    "[PlatformInitializer][resolveDatabaseConfigPath] "
                            + "Database configuration is missing or unreadable"
                            + " | path=" + resolvedPath
            );

            throw new IllegalArgumentException(
                    "Database configuration does not exist or is not readable: "
                            + resolvedPath
            );
        }

        log.debug(() ->
                "[PlatformInitializer][resolveDatabaseConfigPath] "
                        + "Database configuration resolved"
                        + " | path=" + resolvedPath
        );

        return resolvedPath;
    }


    // =========================================================
    // 4. LOAD PLATFORM CONFIGURATION
    // =========================================================

    /**
     * Reads the PLATFORM definition from the configured file.
     *
     * The configuration file remains the source of truth.
     * JVM arguments are not used to reconstruct the Platform record.
     */
    private void initializeParameters(String[] args) {

        log.info(
                "[PlatformInitializer][initializeParameters] "
                        + "Beginning PLATFORM configuration initialization"
        );

        // -----------------------------------------------------
        // 4.1. Parse application arguments
        // -----------------------------------------------------

        Map<String, String> parameters = parseArguments(args);

        // -----------------------------------------------------
        // 4.2. Resolve PLATFORM configuration
        // -----------------------------------------------------

        platformConfigPath = resolveConfigPath(
                required(parameters, "container-conf")
        );

        log.info(() ->
                "[PlatformInitializer][initializeParameters] "
                        + "Loading PLATFORM configuration"
                        + " | path=" + platformConfigPath
        );

        // -----------------------------------------------------
        // 4.3. Parse PLATFORM definition
        // -----------------------------------------------------

        platformDefinition = Objects.requireNonNull(
                new ConfigParser().initializePlatformConfigs(
                        platformConfigPath.toString()
                ),
                "Platform configuration parser returned null"
        );

        log.debug(() ->
                "[PlatformInitializer][initializeParameters] "
                        + "PLATFORM definition loaded"
                        + " | platformId=" + platformDefinition.id()
                        + " | tcpPort=" + platformDefinition.tcpPort()
                        + " | controlPort=" + platformDefinition.controlPort()
                        + " | configuredECs="
                        + platformDefinition.ecConfigs().size()
                        + " | configuredWatchdogs="
                        + platformDefinition.watchdogConfigs().size()
        );

        // -----------------------------------------------------
        // 4.4. Validate supplied container type
        // -----------------------------------------------------

        if (parameters.containsKey("container-type")) {

            String suppliedType = parameters.get("container-type");

            if (!"PLATFORM".equalsIgnoreCase(suppliedType)) {

                log.warn(() ->
                        "[PlatformInitializer][initializeParameters] "
                                + "Invalid container type"
                                + " | suppliedType=" + suppliedType
                );

                throw new IllegalArgumentException(
                        "PlatformInitializer can only initialize PLATFORM"
                );
            }
        }

        // -----------------------------------------------------
        // 4.5. Validate supplied container name
        // -----------------------------------------------------

        if (parameters.containsKey("container-name")
                && !platformDefinition.id().equals(
                parameters.get("container-name")
        )) {

            log.warn(() ->
                    "[PlatformInitializer][initializeParameters] "
                            + "Container name does not match configuration"
                            + " | configuredId=" + platformDefinition.id()
                            + " | suppliedName="
                            + parameters.get("container-name")
            );

            throw new IllegalArgumentException(
                    "Platform name does not match the configuration file"
            );
        }

        // -----------------------------------------------------
        // 4.6. Resolve database configuration
        // -----------------------------------------------------

        databaseConfigPath = resolveDatabaseConfigPath(
                platformDefinition.databaseConfig()
        );

        // -----------------------------------------------------
        // 4.7. Validate PLATFORM definition
        // -----------------------------------------------------

        validatePlatformConfiguration();

        log.info(() ->
                "[PlatformInitializer][initializeParameters] "
                        + "PLATFORM configuration initialized successfully"
                        + " | platformId=" + platformDefinition.id()
                        + " | platformConfig=" + platformConfigPath
                        + " | databaseConfig=" + databaseConfigPath
        );
    }


    // =========================================================
    // 5. VALIDATE PLATFORM CONFIGURATION
    // =========================================================

    private void validatePlatformConfiguration() {

        log.debug(
                "[PlatformInitializer][validatePlatformConfiguration] "
                        + "Beginning configuration validation"
        );

        // -----------------------------------------------------
        // 5.1. PLATFORM identity
        // -----------------------------------------------------

        if (platformDefinition.id() == null
                || platformDefinition.id().isBlank()) {

            log.warn(
                    "[PlatformInitializer][validatePlatformConfiguration] "
                            + "PLATFORM ID is empty"
            );

            throw new IllegalArgumentException(
                    "Platform ID cannot be empty"
            );
        }

        // -----------------------------------------------------
        // 5.2. Application TCP port
        // -----------------------------------------------------

        if (platformDefinition.tcpPort() < 1
                || platformDefinition.tcpPort() > 65535) {

            log.warn(() ->
                    "[PlatformInitializer][validatePlatformConfiguration] "
                            + "Invalid application TCP port"
                            + " | port=" + platformDefinition.tcpPort()
            );

            throw new IllegalArgumentException(
                    "Invalid platform TCP port"
            );
        }

        // -----------------------------------------------------
        // 5.3. Control TCP port
        // -----------------------------------------------------

        if (platformDefinition.controlPort() < 1
                || platformDefinition.controlPort() > 65535) {

            log.warn(() ->
                    "[PlatformInitializer][validatePlatformConfiguration] "
                            + "Invalid control TCP port"
                            + " | port=" + platformDefinition.controlPort()
            );

            throw new IllegalArgumentException(
                    "Invalid platform control port"
            );
        }

        // -----------------------------------------------------
        // 5.4. Port collision
        // -----------------------------------------------------

        if (platformDefinition.tcpPort()
                == platformDefinition.controlPort()) {

            log.warn(() ->
                    "[PlatformInitializer][validatePlatformConfiguration] "
                            + "Application and control ports are identical"
                            + " | port=" + platformDefinition.tcpPort()
            );

            throw new IllegalArgumentException(
                    "Platform TCP port and control port must be different"
            );
        }

        // -----------------------------------------------------
        // 5.5. TCP backlog
        // -----------------------------------------------------

        if (platformDefinition.tcpBacklog() < 1) {

            log.warn(() ->
                    "[PlatformInitializer][validatePlatformConfiguration] "
                            + "TCP backlog must be positive"
                            + " | backlog=" + platformDefinition.tcpBacklog()
            );

            throw new IllegalArgumentException(
                    "TCP backlog must be greater than zero"
            );
        }

        // -----------------------------------------------------
        // 5.6. Thread pool configuration
        // -----------------------------------------------------

        if (platformDefinition.coreThreads() < 0
                || platformDefinition.maxThreads() < 1
                || platformDefinition.coreThreads()
                > platformDefinition.maxThreads()) {

            log.warn(() ->
                    "[PlatformInitializer][validatePlatformConfiguration] "
                            + "Invalid thread pool configuration"
                            + " | coreThreads=" + platformDefinition.coreThreads()
                            + " | maxThreads=" + platformDefinition.maxThreads()
            );

            throw new IllegalArgumentException(
                    "Invalid platform thread configuration"
            );
        }

        // -----------------------------------------------------
        // 5.7. Idle timeout
        // -----------------------------------------------------

        if (platformDefinition.idleTimeout() < 0) {

            log.warn(() ->
                    "[PlatformInitializer][validatePlatformConfiguration] "
                            + "Invalid idle timeout"
                            + " | idleTimeout=" + platformDefinition.idleTimeout()
            );

            throw new IllegalArgumentException(
                    "Idle timeout cannot be negative"
            );
        }

        log.info(() ->
                "[PlatformInitializer][validatePlatformConfiguration] "
                        + "Configuration validation successful"
                        + " | platformId=" + platformDefinition.id()
                        + " | tcpPort=" + platformDefinition.tcpPort()
                        + " | controlPort=" + platformDefinition.controlPort()
                        + " | tcpBacklog=" + platformDefinition.tcpBacklog()
                        + " | coreThreads=" + platformDefinition.coreThreads()
                        + " | maxThreads=" + platformDefinition.maxThreads()
                        + " | idleTimeout=" + platformDefinition.idleTimeout()
        );
    }


    // =========================================================
    // 6. INITIALIZE PLATFORM SERVICES
    // =========================================================

    /**
     * Creates and wires all PLATFORM services.
     *
     * Initialization order:
     *
     * 1. Registries
     * 2. Parsers
     * 3. Database infrastructure
     * 4. Message processing
     * 5. Health monitoring
     * 6. Heartbeat monitoring
     * 7. EC-to-WATCHDOG ownership mapping
     * 8. PlatformService
     * 9. Command handling
     * 10. TCP processors and services
     * 11. Dependency injection
     * 12. Service registration
     */
    private void initializePlatformServices() {

        // -----------------------------------------------------
        // 6.1. Resolve PLATFORM runtime configuration
        // -----------------------------------------------------

        ProcessType processType = ProcessType.PLATFORM;

        int coreThreads = platformDefinition.coreThreads();
        int maxThreads = platformDefinition.maxThreads();
        int idleTimeout = platformDefinition.idleTimeout();

        int tcpPort = platformDefinition.tcpPort();
        int tcpBacklog = platformDefinition.tcpBacklog();
        int controlPort = platformDefinition.controlPort();

        String platformName = platformDefinition.id();

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "Beginning PLATFORM dependency initialization"
                        + " | platformId=" + platformName
                        + " | tcpPort=" + tcpPort
                        + " | controlPort=" + controlPort
                        + " | coreThreads=" + coreThreads
                        + " | maxThreads=" + maxThreads
                        + " | idleTimeout=" + idleTimeout
        );


        // =====================================================
        // STAGE 1. REGISTRIES
        // =====================================================

        log.debug(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Stage 1: Initializing registries"
        );

        CorrelationRegistry correlationRegistry =
                new CorrelationRegistry();

        RetryRegistry retryRegistry =
                new RetryRegistry(5, 300);

        serviceRegistry = new ContainerServiceRegistry();

        log.info(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Registries initialized"
        );


        // =====================================================
        // STAGE 2. PARSERS
        // =====================================================

        log.debug(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Stage 2: Initializing parsers"
        );

        MessageParser messageParser =
                new MessageParser(processType);

        CommandParser commandParser =
                new CommandParser(processType);

        log.debug(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "Parsers initialized"
                        + " | processType=" + processType
        );


        // =====================================================
        // STAGE 3. DATABASE INFRASTRUCTURE
        // =====================================================

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "Stage 3: Initializing database infrastructure"
                        + " | configPath=" + databaseConfigPath
        );

        // Connection pool

        HikariDBConnectionPool hikariDBConnectionPool =
                new HikariDBConnectionPool();

        // Database resource manager

        H2DatabaseResourceManager databaseResourceManager =
                new H2DatabaseResourceManager(
                        databaseConfigPath.toString(),
                        hikariDBConnectionPool
                );

        // Database request handler

        DatabaseRequestHandler databaseRequestHandler =
                new DatabaseRequestHandler(
                        databaseResourceManager
                );

        // Database service

        databaseService = new DatabaseService(
                coreThreads,
                maxThreads,
                idleTimeout,
                databaseResourceManager,
                databaseRequestHandler
        );

        // Database forwarder

        DatabaseForwarder databaseForwarder =
                new DatabaseForwarder(databaseService);

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "Database infrastructure created"
                        + " | serviceType="
                        + databaseService.getClass().getSimpleName()
        );


        // =====================================================
        // STAGE 4. MESSAGE PROCESSING INFRASTRUCTURE
        // =====================================================

        log.info(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Stage 4: Initializing message processing"
        );

        // Response processor

        ResponseProcessor responseProcessor =
                new ResponseProcessor(
                        messageParser,
                        correlationRegistry,
                        retryRegistry
                );

        // Command processor

        CommandProcessor commandProcessor =
                new CommandProcessor(
                        commandParser,
                        responseProcessor,
                        Set.of(
                                "help",
                                "status",
                                "restart services all",
                                "shutdown services all",
                                "kill"
                        )
                );

        // Message processor

        MessageProcessor messageProcessor =
                new MessageProcessor(databaseForwarder);

        // Request processor

        RequestProcessor requestProcessor =
                new RequestProcessor(
                        correlationRegistry,
                        messageParser,
                        commandProcessor,
                        messageProcessor
                );

        // Message service

        messageService = new MessageService(
                coreThreads,
                maxThreads,
                idleTimeout,
                5,
                5,
                60,
                requestProcessor,
                responseProcessor,
                correlationRegistry,
                retryRegistry
        );

        // Complete the command processor's dependency wiring

        commandProcessor.setRequestProcessor(
                requestProcessor
        );

        log.debug(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "Message processing dependencies connected"
                        + " | requestProcessor="
                        + requestProcessor.getClass().getSimpleName()
                        + " | responseProcessor="
                        + responseProcessor.getClass().getSimpleName()
        );

        log.info(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Message processing infrastructure created"
        );


        // =====================================================
        // STAGE 5. HEALTH MONITORING
        // =====================================================

        log.debug(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Stage 5: Initializing health monitoring"
        );

        HealthService healthService =
                new HealthService(
                        serviceRegistry,
                        100,
                        75
                );

        HealthReportAnalyzer healthReportAnalyzer =
                new HealthReportAnalyzer();

        log.debug(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Health monitoring dependencies created"
        );


        // =====================================================
        // STAGE 6. HEARTBEAT MONITORING
        // =====================================================

        log.debug(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Stage 6: Initializing heartbeat monitoring"
        );

        HeartbeatRegistry heartbeatRegistry =
                new HeartbeatRegistry(HEART_BEAT_TTL);

        ExternalHealthMonitor externalHealthMonitor =
                new ExternalHealthMonitor(
                        heartbeatRegistry,
                        platformDefinition.ecConfigs(),
                        messageParser
                );

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "Heartbeat monitoring created"
                        + " | heartbeatTTL=" + HEART_BEAT_TTL
                        + " | monitoredECs="
                        + platformDefinition.ecConfigs().size()
        );


        // =====================================================
        // STAGE 7. EC-TO-WATCHDOG OWNERSHIP MAPPING
        // =====================================================

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "Stage 7: Building EC-to-WATCHDOG ownership map"
                        + " | watchdogCount="
                        + platformDefinition.watchdogConfigs().size()
        );

        Map<String, ConsolidatedWatchdogConfig> ecWatchdogMap =
                new HashMap<>();

        WatchdogConfigParser watchdogConfigParser =
                new WatchdogConfigParser();

        for (ConsolidatedWatchdogConfig watchdogConfig
                : platformDefinition.watchdogConfigs()) {

            log.debug(() ->
                    "[PlatformInitializer][initializePlatformServices] "
                            + "Loading WATCHDOG configuration"
                            + " | watchdogId=" + watchdogConfig.id()
                            + " | configPath=" + watchdogConfig.configPath()
            );

            Watchdog watchdog =
                    watchdogConfigParser.initializeWatchdogConfig(
                            watchdogConfig.configPath()
                    );

            List<ConsolidatedECConfig> ecConfigs =
                    watchdog.ecConfigs();

            log.debug(() ->
                    "[PlatformInitializer][initializePlatformServices] "
                            + "WATCHDOG configuration loaded"
                            + " | watchdogId=" + watchdogConfig.id()
                            + " | ecCount=" + ecConfigs.size()
            );

            for (ConsolidatedECConfig ecConfig : ecConfigs) {

                log.debug(() ->
                        "[PlatformInitializer][initializePlatformServices] "
                                + "Mapping EC to WATCHDOG"
                                + " | ecId=" + ecConfig.ecId()
                                + " | watchdogId=" + watchdogConfig.id()
                );

                ecWatchdogMap.put(
                        ecConfig.ecId(),
                        watchdogConfig
                );
            }
        }

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "EC-to-WATCHDOG ownership map completed"
                        + " | mappedECCount=" + ecWatchdogMap.size()
        );


        // =====================================================
        // STAGE 8. PLATFORM SERVICE
        // =====================================================

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "Stage 8: Creating PlatformService"
                        + " | platformId=" + platformName
        );

        platformService = new PlatformService(
                coreThreads,
                maxThreads,
                idleTimeout,
                tcpPort,
                tcpBacklog,
                controlPort,
                platformName,
                databaseConfigPath.toString(),
                serviceRegistry,
                healthService,
                healthReportAnalyzer,
                platformDefinition.ecConfigs(),
                externalHealthMonitor,
                ecWatchdogMap,
                platformDefinition
        );

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "PlatformService created"
                        + " | platformId=" + platformName
        );


        // =====================================================
        // STAGE 9. PLATFORM COMMAND HANDLING
        // =====================================================

        log.debug(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Stage 9: Initializing command handling"
        );

        PlatformControlHandler platformControlHandler =
                new PlatformControlHandler(
                        commandParser,
                        ecWatchdogMap
                );

        PlatformCommandHandler platformCommandHandler =
                new PlatformCommandHandler(
                        platformService,
                        commandParser,
                        platformControlHandler,
                        externalHealthMonitor
                );

        CommandForwarder commandForwarder =
                new CommandForwarder(platformService);

        messageProcessor.setCommandForwarder(
                commandForwarder
        );

        GUIForwarder guiForwarder =
                new GUIForwarder();

        messageProcessor.setGUIForwarder(
                guiForwarder
        );

        platformService.setCommandHandler(
                platformCommandHandler
        );

        platformService.setMessageDispatcher(
                messageService
        );

        log.debug(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "Command handling dependencies connected"
                        + " | commandHandler="
                        + platformCommandHandler.getClass().getSimpleName()
        );

        log.info(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Platform command handling created"
        );


        // =====================================================
        // STAGE 10. TCP PROCESSORS AND SERVICES
        // =====================================================

        log.info(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Stage 10: Initializing TCP infrastructure"
        );

        // -----------------------------------------------------
        // 10.1. Application TCP processor
        // -----------------------------------------------------

        TCPApplicationProcessor tcpApplicationProcessor =
                new TCPApplicationProcessor(
                        List.of(),
                        messageService,
                        messageParser
                );

        log.debug(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Application TCP processor created"
        );

        // -----------------------------------------------------
        // 10.2. Build TCP peer definitions
        // -----------------------------------------------------

        List<TCPPeer> tcpPeers = new ArrayList<>();

        log.debug(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "Building TCP peers"
                        + " | ecCount="
                        + platformDefinition.ecConfigs().size()
                        + " | watchdogCount="
                        + platformDefinition.watchdogConfigs().size()
        );

        // Execution container peers

        for (ConsolidatedECConfig ecConfig
                : platformDefinition.ecConfigs()) {

            TCPPeer peer = new TCPPeer(
                    ecConfig.ecId(),
                    ecConfig.host(),
                    ecConfig.tcpPort()
            );

            tcpPeers.add(peer);

            log.debug(() ->
                    "[PlatformInitializer][initializePlatformServices] "
                            + "EC TCP peer registered"
                            + " | ecId=" + ecConfig.ecId()
                            + " | host=" + ecConfig.host()
                            + " | port=" + ecConfig.tcpPort()
            );
        }

        // WATCHDOG peers

        for (ConsolidatedWatchdogConfig watchdogConfig
                : platformDefinition.watchdogConfigs()) {

            TCPPeer peer = new TCPPeer(
                    watchdogConfig.id(),
                    watchdogConfig.host(),
                    watchdogConfig.controlPort()
            );

            tcpPeers.add(peer);

            log.debug(() ->
                    "[PlatformInitializer][initializePlatformServices] "
                            + "WATCHDOG TCP peer registered"
                            + " | watchdogId=" + watchdogConfig.id()
                            + " | host=" + watchdogConfig.host()
                            + " | port=" + watchdogConfig.controlPort()
            );
        }

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "TCP peer definitions completed"
                        + " | peerCount=" + tcpPeers.size()
        );

        // -----------------------------------------------------
        // 10.3. Control TCP processor
        // -----------------------------------------------------

        TCPProtocolProcessor tcpProtocolProcessor =
                new TCPProtocolProcessor(
                        TCPConnectionMode.SERVER_ONLY,
                        tcpPeers,
                        messageParser
                );

        log.debug(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "Control TCP processor created"
                        + " | mode=" + TCPConnectionMode.SERVER_ONLY
        );

        // -----------------------------------------------------
        // 10.4. Application TCP service
        // -----------------------------------------------------

        TCPService tcpApplicationService =
                new TCPService(
                        coreThreads,
                        maxThreads,
                        idleTimeout,
                        platformDefinition.tcpBindHost(),
                        tcpPort,
                        tcpBacklog,
                        true,
                        tcpApplicationProcessor
                );

        log.debug(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "Application TCP service created"
                        + " | bindHost="
                        + platformDefinition.tcpBindHost()
                        + " | port=" + tcpPort
                        + " | backlog=" + tcpBacklog
        );

        // -----------------------------------------------------
        // 10.5. Control TCP service
        // -----------------------------------------------------

        TCPService tcpControlService =
                new TCPService(
                        coreThreads,
                        maxThreads,
                        idleTimeout,
                        platformDefinition.controlBindHost(),
                        controlPort,
                        tcpBacklog,
                        true,
                        tcpProtocolProcessor
                );

        log.debug(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "Control TCP service created"
                        + " | bindHost="
                        + platformDefinition.controlBindHost()
                        + " | port=" + controlPort
                        + " | backlog=" + tcpBacklog
        );

        log.info(
                "[PlatformInitializer][initializePlatformServices] "
                        + "TCP infrastructure created"
        );


        // =====================================================
        // STAGE 11. DEPENDENCY INJECTION
        // =====================================================

        log.info(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Stage 11: Connecting service dependencies"
        );

        // Database service -> Message service

        databaseService.setMessageDispatcher(
                messageService
        );

        log.debug(
                "[PlatformInitializer][initializePlatformServices] "
                        + "DatabaseService connected to MessageService"
        );

        // Platform service -> Control channel

        platformService.setControlChannelCommunicator(
                tcpProtocolProcessor
        );

        // Health service -> Platform failure reporter

        healthService.setServiceFailureReporter(
                platformService
        );

        // Platform control handler -> Control channel

        platformControlHandler.setControlChannelCommunicator(
                tcpProtocolProcessor
        );

        // Platform control handler -> Message service

        platformControlHandler.setMessageDispatcher(
                messageService
        );

        log.debug(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Platform control and health dependencies connected"
        );

        // Application processor -> Application TCP service

        tcpApplicationProcessor.setTcpService(
                tcpApplicationService
        );

        // Control processor -> Control TCP service

        tcpProtocolProcessor.setTcpService(
                tcpControlService
        );

        // Control processor -> Platform service

        tcpProtocolProcessor.setControlMessageHandler(
                platformService
        );

        log.debug(
                "[PlatformInitializer][initializePlatformServices] "
                        + "TCP processors connected to their services"
        );

        log.info(
                "[PlatformInitializer][initializePlatformServices] "
                        + "All service dependencies connected"
        );


        // =====================================================
        // STAGE 12. SERVICE REGISTRATION
        // =====================================================

        log.info(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Stage 12: Registering PLATFORM services"
        );

        serviceRegistry.registerService(
                databaseService
        );

        log.debug(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Registered DatabaseService"
        );

        serviceRegistry.registerService(
                messageService
        );

        log.debug(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Registered MessageService"
        );

        serviceRegistry.registerService(
                "application-tcp",
                tcpApplicationService
        );

        log.debug(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Registered application TCP service"
        );

        serviceRegistry.registerService(
                "control-tcp",
                tcpControlService
        );

        log.debug(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Registered control TCP service"
        );

        serviceRegistry.registerService(
                healthService
        );

        log.debug(
                "[PlatformInitializer][initializePlatformServices] "
                        + "Registered HealthService"
        );

        log.info(
                "[PlatformInitializer][initializePlatformServices] "
                        + "PLATFORM service registration completed"
                        + " | services=[DatabaseService, MessageService, "
                        + "application-tcp, control-tcp, HealthService]"
        );


        // =====================================================
        // INITIALIZATION COMPLETE
        // =====================================================

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] "
                        + "PLATFORM dependency initialization completed"
                        + " | platformId=" + platformName
        );
    }


    // =========================================================
    // 7. INITIALIZE PLATFORM JVM
    // =========================================================

    /**
     * Initializes the PLATFORM exactly once.
     *
     * This method creates the process context and assembles
     * all PLATFORM services.
     *
     * It does not start the PLATFORM lifecycle.
     */
    private void initializePlatform(String[] args) {

        log.info(() ->
                "[PlatformInitializer][initializePlatform] "
                        + "PLATFORM initialization requested"
                        + " | alreadyAttempted=" + initializationAttempted
        );

        // -----------------------------------------------------
        // 7.1. Prevent duplicate initialization
        // -----------------------------------------------------

        if (initializationAttempted) {

            log.warn(
                    "[PlatformInitializer][initializePlatform] "
                            + "Duplicate initialization attempt rejected"
            );

            throw new IllegalStateException(
                    "PlatformInitializer cannot be initialized more than once"
            );
        }

        initializationAttempted = true;

        // -----------------------------------------------------
        // 7.2. Load PLATFORM configuration
        // -----------------------------------------------------

        initializeParameters(args);

        // -----------------------------------------------------
        // 7.3. Initialize process identity
        // -----------------------------------------------------

        log.info(() ->
                "[PlatformInitializer][initializePlatform] "
                        + "Initializing PLATFORM process context"
                        + " | platformId=" + platformDefinition.id()
        );

        ProcessContext.initialize(
                platformDefinition.id()
        );

        log.debug(() ->
                "[PlatformInitializer][initializePlatform] "
                        + "Process context initialized"
                        + " | processName=" + platformDefinition.id()
        );

        // -----------------------------------------------------
        // 7.4. Assemble PLATFORM services
        // -----------------------------------------------------

        initializePlatformServices();

        log.info(() ->
                "[PlatformInitializer][initializePlatform] "
                        + "PLATFORM JVM initialization completed"
                        + " | platformId=" + platformDefinition.id()
        );
    }


    // =========================================================
    // 8. START PLATFORM
    // =========================================================

    /**
     * Starts the PLATFORM lifecycle.
     *
     * The actual service startup sequence is owned by
     * PlatformService.
     */
    public void startPlatform() {

        log.info(
                "[PlatformInitializer][startPlatform] "
                        + "PLATFORM startup requested"
        );

        Objects.requireNonNull(
                platformService,
                "PlatformService has not been initialized"
        );

        log.debug(() ->
                "[PlatformInitializer][startPlatform] "
                        + "Invoking PlatformService.start()"
                        + " | platformId=" + platformDefinition.id()
        );

        platformService.start();

        log.info(() ->
                "[PlatformInitializer][startPlatform] "
                        + "PLATFORM startup completed"
                        + " | platformId=" + platformDefinition.id()
        );
    }


    // =========================================================
    // 9. JVM ENTRY POINT
    // =========================================================

    public static void main(String[] args) {

        log.info(() ->
                "[PlatformInitializer][main] "
                        + "PLATFORM JVM entry point invoked"
                        + " | argumentCount="
                        + (args == null ? "null" : args.length)
        );

        // -----------------------------------------------------
        // 9.1. Initialize runtime
        // -----------------------------------------------------

        ContainerRuntime.initialize();

        log.debug(
                "[PlatformInitializer][main] "
                        + "Container runtime initialized"
        );

        try {

            // -------------------------------------------------
            // 9.2. Create initializer
            // -------------------------------------------------

            PlatformInitializer initializer =
                    new PlatformInitializer();

            log.debug(
                    "[PlatformInitializer][main] "
                            + "PlatformInitializer instance created"
            );

            // -------------------------------------------------
            // 9.3. Initialize PLATFORM
            // -------------------------------------------------

            initializer.initializePlatform(args);

            // -------------------------------------------------
            // 9.4. Start PLATFORM lifecycle
            // -------------------------------------------------

            initializer.startPlatform();

            log.info(
                    "[PlatformInitializer][main] "
                            + "PLATFORM startup sequence completed successfully"
            );

        } catch (Exception exception) {

            log.error(
                    "[PlatformInitializer][main] "
                            + "Fatal PLATFORM initialization or startup failure",
                    exception
            );

            throw new IllegalStateException(
                    "PLATFORM JVM initialization failed",
                    exception
            );
        }
    }
}
