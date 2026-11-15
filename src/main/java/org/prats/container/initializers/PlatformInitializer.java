package org.prats.container.initializers;

import org.prats.database.databaseUtilities.HikariDBConnectionPool.HikariDBConnectionPool;
import org.prats.database.h2Database.H2DatabaseResourceManager;

import org.prats.definitions.classes.ProcessContext;
import org.prats.definitions.enums.ProcessType;
import org.prats.definitions.enums.TCPConnectionMode;
import org.prats.definitions.records.*;

import org.prats.definitions.interfaces.ServiceRegistry;

import org.prats.forwarders.CommandForwarder;
import org.prats.forwarders.DatabaseForwarder;

import org.prats.handlers.DatabaseRequestHandler;
import org.prats.handlers.PlatformCommandHandler;

import org.prats.handlers.PlatformControlHandler;
import org.prats.parsers.MessageParser;
import org.prats.parsers.CommandParser;
import org.prats.parsers.configParser.ConfigParser;

import org.prats.parsers.configParser.WatchdogConfigParser;
import org.prats.processors.*;

import org.prats.registries.ContainerServiceRegistry;
import org.prats.registries.CorrelationRegistry;
import org.prats.registries.HeartbeatRegistry;
import org.prats.registries.RetryRegistry;

import org.prats.services.ExternalHealthMonitor;
import org.prats.services.databaseService.DatabaseService;
import org.prats.services.healthCheckService.HealthService;
import org.prats.services.messageService.MessageService;
import org.prats.services.containerServices.PlatformService;
import org.prats.services.tcpService.TCPService;

import org.prats.utilities.ApplicationPaths;
import org.prats.utilities.ContainerRuntime;
import org.prats.utilities.HealthReportAnalyzer;
import org.prats.loggers.EngineLogger;

import java.nio.file.Files;
import java.nio.file.Path;

import java.util.*;


public class PlatformInitializer {

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

    private static final EngineLogger log =
            EngineLogger.getLogger(PlatformInitializer.class);


    private boolean initializationAttempted;


    /*
     * Parses application arguments.
     *
     * Supports:
     * --key=value
     * --key value
     */
    private Map<String, String> parseArguments(String[] args) {

        log.debug(() ->
                "[PlatformInitializer][parseArguments] Parsing PLATFORM JVM arguments"
                        + " | argumentCount=" + (args == null ? "null" : args.length)
        );

        if (args == null) {

            log.warn(() ->
                    "[PlatformInitializer][parseArguments] Argument array is null"
            );

            throw new IllegalArgumentException(
                    "Platform arguments cannot be null"
            );
        }

        Map<String, String> parameters = new LinkedHashMap<>();

        for (int i = 0; i < args.length; i++) {

            String argument = args[i];

            if (argument == null || !argument.startsWith("--")) {

                log.warn(() ->
                        "[PlatformInitializer][parseArguments] Invalid argument format"
                                + " | argument=" + argument
                );

                throw new IllegalArgumentException(
                        "Invalid argument: " + argument
                );
            }

            int separator = argument.indexOf('=');

            String key;
            String value;

            if (separator >= 0) {

                key = argument.substring(2, separator);

                value = argument.substring(separator + 1);

            } else {

                key = argument.substring(2);

                if (i + 1 >= args.length ||
                        args[i + 1].startsWith("--")) {

                    log.warn(() ->
                            "[PlatformInitializer][parseArguments] Argument value is missing"
                                    + " | argument=" + key
                    );

                    throw new IllegalArgumentException(
                            "Missing value for argument: --" + key
                    );
                }

                value = args[++i];
            }

            if (!SUPPORTED_ARGUMENTS.contains(key)) {

                log.warn(() ->
                        "[PlatformInitializer][parseArguments] Unsupported argument supplied"
                                + " | argument=" + key
                );

                throw new IllegalArgumentException(
                        "Unknown platform argument: --" + key
                );
            }

            if (value == null || value.isBlank()) {

                log.warn(() ->
                        "[PlatformInitializer][parseArguments] Empty argument value supplied"
                                + " | argument=" + key
                );

                throw new IllegalArgumentException(
                        "Empty value supplied for argument: --" + key
                );
            }

            if (parameters.putIfAbsent(key, value) != null) {

                log.warn(() ->
                        "[PlatformInitializer][parseArguments] Duplicate argument supplied"
                                + " | argument=" + key
                );

                throw new IllegalArgumentException(
                        "Duplicate platform argument: --" + key
                );
            }
        }

        log.debug(() ->
                "[PlatformInitializer][parseArguments] Argument parsing completed"
                        + " | parsedArguments=" + parameters.keySet()
        );

        return parameters;
    }


    /*
     * Retrieves a mandatory parameter.
     */
    private String required(
            Map<String, String> parameters,
            String key
    ) {

        log.debug(() ->
                "[PlatformInitializer][required] Resolving mandatory argument"
                        + " | argument=" + key
        );

        String value = parameters.get(key);

        if (value == null || value.isBlank()) {

            log.warn(() ->
                    "[PlatformInitializer][required] Mandatory argument is missing"
                            + " | argument=" + key
            );

            throw new IllegalArgumentException(
                    "Missing required argument: --" + key
            );
        }

        String resolvedValue = value.trim();

        log.debug(() ->
                "[PlatformInitializer][required] Mandatory argument resolved"
                        + " | argument=" + key
        );

        return resolvedValue;
    }


    /*
     * Resolves a configuration path.
     */
    private Path resolveConfigPath(String configuredPath) {

        log.debug(() ->
                "[PlatformInitializer][resolveConfigPath] Resolving platform configuration path"
                        + " | configuredPath=" + configuredPath
        );

        Path path = ApplicationPaths.resolve(configuredPath);

        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            log.warn(() ->
                    "[PlatformInitializer][resolveConfigPath] Platform configuration file is missing or unreadable"
                            + " | resolvedPath=" + path
            );

            throw new IllegalArgumentException(
                    "Configuration file does not exist or is not readable: "
                            + path
            );
        }

        log.debug(() ->
                "[PlatformInitializer][resolveConfigPath] Platform configuration path resolved"
                        + " | resolvedPath=" + path
        );

        return path;
    }


    /*
     * Resolves database configuration relative to the
     * platform configuration file when necessary.
     */
    private Path resolveDatabaseConfigPath(String configuredPath) {

        log.debug(() ->
                "[PlatformInitializer][resolveDatabaseConfigPath] Resolving database configuration path"
                        + " | configuredPath=" + configuredPath
        );

        Objects.requireNonNull(
                configuredPath,
                "Database configuration path cannot be null"
        );

        Path path = Path.of(configuredPath);

        // Parser may already have resolved this path.
        if (!path.isAbsolute()) {
            path = ApplicationPaths.resolve(configuredPath);
        }

        path = path.toAbsolutePath().normalize();

        final Path resolvedDatabasePath = path;

        if (!Files.isRegularFile(resolvedDatabasePath) || !Files.isReadable(resolvedDatabasePath)) {
            log.warn(() ->
                    "[PlatformInitializer][resolveDatabaseConfigPath] Database configuration file is missing or unreadable"
                            + " | resolvedPath=" + resolvedDatabasePath
            );

            throw new IllegalArgumentException(
                    "Database configuration does not exist or is not readable: "
                            + resolvedDatabasePath
            );
        }

        log.debug(() ->
                "[PlatformInitializer][resolveDatabaseConfigPath] Database configuration path resolved"
                        + " | resolvedPath=" + resolvedDatabasePath
        );

        return resolvedDatabasePath;
    }


    /*
     * Reads the platform definition.
     *
     * The platform configuration is the source of truth.
     * Individual JVM arguments are not used to reconstruct
     * the Platform object.
     */
    private void initializeParameters(String[] args) {

        log.info(() ->
                "[PlatformInitializer][initializeParameters] Beginning PLATFORM configuration initialization"
        );

        Map<String, String> parameters = parseArguments(args);

        platformConfigPath = resolveConfigPath(
                required(parameters, "container-conf")
        );

        log.info(() ->
                "[PlatformInitializer][initializeParameters] Loading platform definition"
                        + " | platformConfigPath=" + platformConfigPath
        );

        platformDefinition = Objects.requireNonNull(
                new ConfigParser().initializePlatformConfigs(
                        platformConfigPath.toString()
                ),
                "Platform configuration parser returned null"
        );

        log.debug(() ->
                "[PlatformInitializer][initializeParameters] Platform definition loaded"
                        + " | platformId=" + platformDefinition.id()
                        + " | tcpPort=" + platformDefinition.tcpPort()
                        + " | controlPort=" + platformDefinition.controlPort()
                        + " | ecCount=" + platformDefinition.ecConfigs().size()
                        + " | watchdogCount=" + platformDefinition.watchdogConfigs().size()
        );

        if (parameters.containsKey("container-type")) {

            String suppliedType = parameters.get("container-type");

            if (!"PLATFORM".equalsIgnoreCase(suppliedType)) {

                log.warn(() ->
                        "[PlatformInitializer][initializeParameters] Supplied container type is not PLATFORM"
                                + " | suppliedType=" + suppliedType
                );

                throw new IllegalArgumentException(
                        "PlatformInitializer can only initialize PLATFORM"
                );
            }
        }

        if (parameters.containsKey("container-name")
                && !platformDefinition.id().equals(
                parameters.get("container-name"))) {

            log.warn(() ->
                    "[PlatformInitializer][initializeParameters] Supplied container name does not match the platform configuration"
                            + " | configuredPlatformId=" + platformDefinition.id()
                            + " | suppliedContainerName=" + parameters.get("container-name")
            );

            throw new IllegalArgumentException(
                    "Platform name does not match the configuration file"
            );
        }

        databaseConfigPath = resolveDatabaseConfigPath(
                platformDefinition.databaseConfig()
        );

        log.debug(() ->
                "[PlatformInitializer][initializeParameters] Database configuration resolved"
                        + " | databaseConfigPath=" + databaseConfigPath
        );

        validatePlatformConfiguration();

        log.info(() ->
                "[PlatformInitializer] Loaded platform configuration: "
                        + platformDefinition.id()
                        + " | platformConfigPath=" + platformConfigPath
                        + " | databaseConfigPath=" + databaseConfigPath
        );
    }


    /*
     * Validates platform configuration.
     */
    private void validatePlatformConfiguration() {

        log.debug(() ->
                "[PlatformInitializer][validatePlatformConfiguration] Validating platform configuration"
                        + " | platformId=" + platformDefinition.id()
        );

        if (platformDefinition.id() == null
                || platformDefinition.id().isBlank()) {

            log.warn(() ->
                    "[PlatformInitializer][validatePlatformConfiguration] Platform ID is empty"
            );

            throw new IllegalArgumentException(
                    "Platform ID cannot be empty"
            );
        }

        if (platformDefinition.tcpPort() < 1
                || platformDefinition.tcpPort() > 65535) {

            log.warn(() ->
                    "[PlatformInitializer][validatePlatformConfiguration] Platform TCP port is outside the valid range"
                            + " | tcpPort=" + platformDefinition.tcpPort()
            );

            throw new IllegalArgumentException(
                    "Invalid platform TCP port"
            );
        }

        if (platformDefinition.controlPort() < 1
                || platformDefinition.controlPort() > 65535) {

            log.warn(() ->
                    "[PlatformInitializer][validatePlatformConfiguration] Platform control port is outside the valid range"
                            + " | controlPort=" + platformDefinition.controlPort()
            );

            throw new IllegalArgumentException(
                    "Invalid platform control port"
            );
        }

        if (platformDefinition.tcpPort()
                == platformDefinition.controlPort()) {

            log.warn(() ->
                    "[PlatformInitializer][validatePlatformConfiguration] Application and control TCP ports are identical"
                            + " | tcpPort=" + platformDefinition.tcpPort()
                            + " | controlPort=" + platformDefinition.controlPort()
            );

            throw new IllegalArgumentException(
                    "Platform TCP port and control port must be different"
            );
        }

        if (platformDefinition.tcpBacklog() < 1) {

            log.warn(() ->
                    "[PlatformInitializer][validatePlatformConfiguration] TCP backlog must be positive"
                            + " | tcpBacklog=" + platformDefinition.tcpBacklog()
            );

            throw new IllegalArgumentException(
                    "TCP backlog must be greater than zero"
            );
        }

        if (platformDefinition.coreThreads() < 0
                || platformDefinition.maxThreads() < 1
                || platformDefinition.coreThreads()
                > platformDefinition.maxThreads()) {

            log.warn(() ->
                    "[PlatformInitializer][validatePlatformConfiguration] Platform thread configuration is invalid"
                            + " | coreThreads=" + platformDefinition.coreThreads()
                            + " | maxThreads=" + platformDefinition.maxThreads()
            );

            throw new IllegalArgumentException(
                    "Invalid platform thread configuration"
            );
        }

        if (platformDefinition.idleTimeout() < 0) {

            log.warn(() ->
                    "[PlatformInitializer][validatePlatformConfiguration] Platform idle timeout cannot be negative"
                            + " | idleTimeout=" + platformDefinition.idleTimeout()
            );

            throw new IllegalArgumentException(
                    "Idle timeout cannot be negative"
            );
        }

        log.info(() ->
                "[PlatformInitializer][validatePlatformConfiguration] Platform configuration validation completed"
                        + " | platformId=" + platformDefinition.id()
                        + " | tcpPort=" + platformDefinition.tcpPort()
                        + " | controlPort=" + platformDefinition.controlPort()
                        + " | tcpBacklog=" + platformDefinition.tcpBacklog()
                        + " | coreThreads=" + platformDefinition.coreThreads()
                        + " | maxThreads=" + platformDefinition.maxThreads()
                        + " | idleTimeout=" + platformDefinition.idleTimeout()
        );
    }


    /*
     * Initializes all PLATFORM services and dependencies.
     *
     * This is the composition root for the PLATFORM JVM.
     */
    private void initializePlatformServices() {

        ProcessType processType = ProcessType.PLATFORM;

        int coreThreads = platformDefinition.coreThreads();
        int maxThreads = platformDefinition.maxThreads();
        int idleTimeout = platformDefinition.idleTimeout();

        int tcpPort = platformDefinition.tcpPort();
        int tcpBacklog = platformDefinition.tcpBacklog();
        int controlPort = platformDefinition.controlPort();

        String platformName = platformDefinition.id();

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] Initializing PLATFORM dependencies"
                        + " | platformId=" + platformName
                        + " | tcpPort=" + tcpPort
                        + " | controlPort=" + controlPort
                        + " | coreThreads=" + coreThreads
                        + " | maxThreads=" + maxThreads
                        + " | idleTimeout=" + idleTimeout
                        + " | databaseConfigPath=" + databaseConfigPath
        );

        // -------------------------------------------------
        // 1. Registries
        // -------------------------------------------------

        CorrelationRegistry correlationRegistry =
                new CorrelationRegistry();

        RetryRegistry retryRegistry =
                new RetryRegistry(5, 300);

        serviceRegistry = new ContainerServiceRegistry();

        log.debug(() ->
                "[PlatformInitializer][initializePlatformServices] Registries initialized"
                        + " | correlationRegistry=" + correlationRegistry.getClass().getSimpleName()
                        + " | retryRegistry=" + retryRegistry.getClass().getSimpleName()
                        + " | serviceRegistry=" + serviceRegistry.getClass().getSimpleName()
        );

        // -------------------------------------------------
        // 2. Parsers
        // -------------------------------------------------

        MessageParser messageParser =
                new MessageParser(processType);

        CommandParser commandParser =
                new CommandParser(processType);

        log.debug(() ->
                "[PlatformInitializer][initializePlatformServices] Message and command parsers initialized"
                        + " | processType=" + processType
        );

        // -------------------------------------------------
        // 3. Database infrastructure
        // -------------------------------------------------

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] Initializing database infrastructure"
                        + " | databaseConfigPath=" + databaseConfigPath
        );

        HikariDBConnectionPool hikariDBConnectionPool =
                new HikariDBConnectionPool();

        H2DatabaseResourceManager databaseResourceManager =
                new H2DatabaseResourceManager(
                        databaseConfigPath.toString(),
                        hikariDBConnectionPool
                );

        DatabaseRequestHandler databaseRequestHandler =
                new DatabaseRequestHandler(
                        databaseResourceManager
                );

        databaseService =
                new DatabaseService(
                        coreThreads,
                        maxThreads,
                        idleTimeout,
                        databaseResourceManager,
                        databaseRequestHandler
                );

        DatabaseForwarder databaseForwarder =
                new DatabaseForwarder(databaseService);

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] Database infrastructure initialized"
                        + " | databaseService=" + databaseService.getClass().getSimpleName()
        );

        // -------------------------------------------------
        // 4. Message processing
        // -------------------------------------------------

        ResponseProcessor responseProcessor =
                new ResponseProcessor(
                        messageParser,
                        correlationRegistry,
                        retryRegistry
                );

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

        MessageProcessor messageProcessor =
                new MessageProcessor(databaseForwarder);

        RequestProcessor requestProcessor =
                new RequestProcessor(
                        correlationRegistry,
                        messageParser,
                        commandProcessor,
                        messageProcessor
                );

        messageService =
                new MessageService(
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

        commandProcessor.setRequestProcessor(requestProcessor);

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] Message-processing pipeline initialized"
                        + " | messageService=" + messageService.getClass().getSimpleName()
                        + " | requestProcessor=" + requestProcessor.getClass().getSimpleName()
                        + " | responseProcessor=" + responseProcessor.getClass().getSimpleName()
        );

        // -------------------------------------------------
        // 5. Health monitoring
        // -------------------------------------------------

        HealthService healthService =
                new HealthService(
                        serviceRegistry,
                        100,
                        75
                );

        HealthReportAnalyzer healthReportAnalyzer =
                new HealthReportAnalyzer();

        log.debug(() ->
                "[PlatformInitializer][initializePlatformServices] Health monitoring dependencies initialized"
                        + " | healthService=" + healthService.getClass().getSimpleName()
        );

        // -------------------------------------------------
        // 6. Heartbeat monitoring
        // -------------------------------------------------

        HeartbeatRegistry heartbeatRegistry =
                new HeartbeatRegistry(HEART_BEAT_TTL);



        ExternalHealthMonitor externalHealthMonitor =
                new ExternalHealthMonitor(
                        heartbeatRegistry,
                        platformDefinition.ecConfigs(),
                        messageParser
                );

        log.debug(() ->
                "[PlatformInitializer][initializePlatformServices] Heartbeat monitoring initialized"
                        + " | heartbeatTtlSeconds=" + HEART_BEAT_TTL
                        + " | configuredECs=" + platformDefinition.ecConfigs().size()
        );

        // -------------------------------------------------
        // 7. ECWatchdogMap
        // -------------------------------------------------

        Map<String, ConsolidatedWatchdogConfig> ecWatchdogMap =
                new HashMap<>();

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] Building EC-to-WATCHDOG ownership map"
                        + " | watchdogCount=" + platformDefinition.watchdogConfigs().size()
        );

        for (ConsolidatedWatchdogConfig watchdogConfig: platformDefinition.watchdogConfigs()) {

            log.debug(() ->
                    "[PlatformInitializer][initializePlatformServices] Loading WATCHDOG configuration"
                            + " | watchdogId=" + watchdogConfig.id()
                            + " | configPath=" + watchdogConfig.configPath()
            );

            Watchdog watchdog   = new WatchdogConfigParser().initializeWatchdogConfig(watchdogConfig.configPath());

            List<ConsolidatedECConfig> ecConfigs = watchdog.ecConfigs();

            log.debug(() ->
                    "[PlatformInitializer][initializePlatformServices] WATCHDOG configuration loaded"
                            + " | watchdogId=" + watchdogConfig.id()
                            + " | ecCount=" + ecConfigs.size()
            );

            for (ConsolidatedECConfig ecConfig: ecConfigs) {

                log.debug(() ->
                        "[PlatformInitializer][initializePlatformServices] Associating EC with WATCHDOG"
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
                "[PlatformInitializer][initializePlatformServices] EC-to-WATCHDOG ownership map completed"
                        + " | mappedECCount=" + ecWatchdogMap.size()
        );

        // -------------------------------------------------
        // 8. Platform service
        // -------------------------------------------------

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] Initializing PlatformService"
                        + " | platformId=" + platformName
        );

        platformService =
                new PlatformService(
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
                "[PlatformInitializer][initializePlatformServices] PlatformService initialized"
                        + " | platformId=" + platformName
        );

        // -------------------------------------------------
        // 7. Platform command handling
        // -------------------------------------------------

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

        messageProcessor.setCommandForwarder(commandForwarder);

        org.prats.security.AccountManager userAccounts =
                new org.prats.security.AccountManager(messageService, messageParser);
        org.prats.handlers.GUIHandler guiHandler = new org.prats.handlers.GUIHandler(
                messageService, messageParser,
                () -> platformDefinition.ecConfigs().stream().map(ConsolidatedECConfig::ecId).toList());
        org.prats.services.guiService.GUIService guiService =
                new org.prats.services.guiService.GUIService(guiHandler, messageService);
        messageProcessor.setGUIForwarder(new org.prats.forwarders.GUIForwarder(guiService));

        org.prats.services.authenticationService.AuthenticationService authenticationService =
                new org.prats.services.authenticationService.AuthenticationService(
                        new org.prats.handlers.AuthenticationHandler(userAccounts, messageParser), messageService);
        messageProcessor.setAuthenticationForwarder(new org.prats.forwarders.AuthenticationForwarder(authenticationService));

        platformService.setCommandHandler(
                platformCommandHandler
        );

        platformService.setMessageDispatcher(
                messageService
        );

        log.debug(() ->
                "[PlatformInitializer][initializePlatformServices] Platform command handling configured"
                        + " | commandHandler=" + platformCommandHandler.getClass().getSimpleName()
                        + " | messageDispatcher=" + messageService.getClass().getSimpleName()
        );

        // -------------------------------------------------
        // 8. TCP processors
        // -------------------------------------------------

        TCPApplicationProcessor tcpApplicationProcessor =
                new TCPApplicationProcessor(
                        List.of(),
                        messageService,
                        messageParser
                );
        tcpApplicationProcessor.setGUIIngressEnabled(true);


        List<TCPPeer> tcpPeers =
                new ArrayList<>();

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] Building TCP peer definitions"
                        + " | ecCount=" + platformDefinition.ecConfigs().size()
                        + " | watchdogCount=" + platformDefinition.watchdogConfigs().size()
        );

        for(ConsolidatedECConfig ecConfig: platformDefinition.ecConfigs()) {
            log.debug(() ->
                    "[PlatformInitializer][initializePlatformServices] Adding EC TCP peer"
                            + " | ecId=" + ecConfig.ecId()
                            + " | host=" + ecConfig.host()
                            + " | tcpPort=" + ecConfig.tcpPort()
            );

            tcpPeers.add(
                    new TCPPeer(
                            ecConfig.ecId(),
                            ecConfig.host(),
                            ecConfig.tcpPort()
                    )
            );
        }

        for(ConsolidatedWatchdogConfig watchdogConfig: platformDefinition.watchdogConfigs()) {
            log.debug(() ->
                    "[PlatformInitializer][initializePlatformServices] Adding WATCHDOG TCP peer"
                            + " | watchdogId=" + watchdogConfig.id()
                            + " | host=" + watchdogConfig.host()
                            + " | controlPort=" + watchdogConfig.controlPort()
            );

            tcpPeers.add(
                    new TCPPeer(
                            watchdogConfig.id(),
                            watchdogConfig.host(),
                            watchdogConfig.controlPort()
                    )
            );
        }

        TCPControlProcessor TCPControlProcessor
                = new TCPControlProcessor(
                TCPConnectionMode.SERVER_ONLY,
                tcpPeers,
                messageParser
        );


        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] TCP processor initialized"
                        + " | peerCount=" + tcpPeers.size()
                        + " | controlProcessor=" + TCPControlProcessor.getClass().getSimpleName()
        );

        // -------------------------------------------------
        // 9. TCP Control service
        //
        // Container/watchdog communication and management
        // -------------------------------------------------


        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] Initializing control TCP service"
                        + " | port=" + controlPort
                        + " | backlog=" + tcpBacklog
        );

        TCPService newTCPApplicationService
                = new TCPService(
                coreThreads,
                maxThreads,
                idleTimeout,
                platformDefinition.tcpBindHost(),
                tcpPort,
                tcpBacklog,
                true,
                tcpApplicationProcessor
        );

        TCPService newTCPControlService
                = new TCPService(
                coreThreads,
                maxThreads,
                idleTimeout,
                platformDefinition.controlBindHost(),
                controlPort,
                tcpBacklog,
                true,
                TCPControlProcessor
        );

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] control TCP service initialized"
                        + " | applicationPort=" + tcpPort
                        + " | controlPort=" + controlPort
        );

        // -------------------------------------------------
        // 11. Dependency injection
        // -------------------------------------------------

        databaseService.setMessageDispatcher(messageService);

        platformService.setControlChannelCommunicator(
                TCPControlProcessor
        );

        healthService.setServiceFailureReporter(
                platformService
        );

        platformControlHandler.setControlChannelCommunicator(
                TCPControlProcessor
        );

        platformControlHandler.setMessageDispatcher(
                messageService
        );

        tcpApplicationProcessor.setTcpService(newTCPApplicationService);
        TCPControlProcessor.setTcpService(newTCPControlService);
        TCPControlProcessor.setControlMessageHandler(platformService);
        org.prats.handlers.JVMDiagnosticsHandler localDiagnostics = new org.prats.handlers.JVMDiagnosticsHandler(
                messageParser,
                () -> serviceRegistry.getAllServices().stream().filter(TCPService.class::isInstance).map(TCPService.class::cast).toList(),
                () -> healthService.getConsolidatedHealthReport().overallStatus());
        localDiagnostics.setRegistryMetrics(() -> Map.of("correlations", correlationRegistry.getSize(),
                "correlationLimit", correlationRegistry.capacity(), "correlationRejections", correlationRegistry.rejectedCount()));
        localDiagnostics.setServiceMetrics(() -> healthService.getConsolidatedHealthReport().services());
        Map<String, String> diagnosticTargets = new LinkedHashMap<>();
        diagnosticTargets.put(platformDefinition.id(), "PLATFORM");
        platformDefinition.ecConfigs().forEach(ec -> diagnosticTargets.put(ec.ecId(), "EC"));
        platformDefinition.watchdogConfigs().forEach(watchdog -> diagnosticTargets.put(watchdog.id(), "WATCHDOG"));
        org.prats.services.diagnosticsService.DiagnosticsService diagnosticsService =
                new org.prats.services.diagnosticsService.DiagnosticsService(localDiagnostics, messageService);
        messageProcessor.setDiagnosticsForwarder(new org.prats.forwarders.DiagnosticsForwarder(platformDefinition.id(),
                diagnosticTargets.keySet(), diagnosticsService, TCPControlProcessor, messageService, messageParser));
        guiHandler.setDiagnostics(new org.prats.handlers.PlatformDiagnostics(diagnosticTargets, messageService, messageParser));


        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] Dependency injection completed"
                        + " | databaseDispatcher=" + messageService.getClass().getSimpleName()
                        + " | platformControlCommunicator=" + TCPControlProcessor.getClass().getSimpleName()
        );

        // -------------------------------------------------
        // 12. Service registration
        // -------------------------------------------------

        serviceRegistry.registerService(databaseService);

        serviceRegistry.registerService(messageService);
        serviceRegistry.registerService(guiService);
        serviceRegistry.registerService(authenticationService);
        serviceRegistry.registerService(diagnosticsService);

        serviceRegistry.registerService(
                "application-tcp",
                newTCPApplicationService
        );

        serviceRegistry.registerService(
                "control-tcp",
                newTCPControlService
        );

        serviceRegistry.registerService(healthService);

        log.info(() ->
                "[PlatformInitializer][initializePlatformServices] PLATFORM service registration completed"
                        + " | registeredServices=[databaseService, messageService, application-tcp, control-tcp, healthService]"
        );

        log.info(() ->
                "[PlatformInitializer] Platform dependencies initialized."
        );
    }


    /*
     * Initializes the PLATFORM JVM.
     */
    private void initializePlatform(String[] args) {

        log.info(() ->
                "[PlatformInitializer][initializePlatform] PLATFORM JVM initialization requested"
                        + " | initializationAttempted=" + initializationAttempted
        );

        if (initializationAttempted) {

            log.warn(() ->
                    "[PlatformInitializer][initializePlatform] Initialization has already been attempted"
            );

            throw new IllegalStateException(
                    "PlatformInitializer cannot be initialized more than once"
            );
        }

        initializationAttempted = true;

        initializeParameters(args);

        log.info(() ->
                "[PlatformInitializer][initializePlatform] Initializing process context"
                        + " | platformId=" + platformDefinition.id()
        );

        ProcessContext.initialize(platformDefinition.id());

        log.debug(() ->
                "[PlatformInitializer][initializePlatform] Process context initialized"
                        + " | platformId=" + platformDefinition.id()
        );

        initializePlatformServices();

        log.info(() ->
                "[PlatformInitializer][initializePlatform] PLATFORM JVM dependencies are initialized"
                        + " | platformId=" + platformDefinition.id()
        );
    }


    /*
     * Starts the PLATFORM lifecycle.
     */
    public void startPlatform() {

        log.info(() ->
                "[PlatformInitializer][startPlatform] PLATFORM startup requested"
                        + " | platformServiceInitialized=" + (platformService != null)
                        + " | platformId=" + (platformDefinition == null ? "unavailable" : platformDefinition.id())
        );

        Objects.requireNonNull(
                platformService,
                "PlatformService has not been initialized"
        );

        log.info(() ->
                "[PlatformInitializer] Starting PLATFORM: "
                        + platformDefinition.id()
        );

        log.debug(() ->
                "[PlatformInitializer][startPlatform] Invoking PlatformService.start()"
                        + " | platformId=" + platformDefinition.id()
        );

        platformService.start();

        log.info(() ->
                "[PlatformInitializer] PLATFORM startup completed."
                        + " | platformId=" + platformDefinition.id()
        );
    }


    /*
     * JVM entry point.
     */
    public static void main(String[] args) {

        log.info(() ->
                "[PlatformInitializer][main] PLATFORM JVM entry point invoked"
                        + " | argumentCount=" + (args == null ? "null" : args.length)
        );

        ContainerRuntime.initialize();

        log.debug(() ->
                "[PlatformInitializer][main] Container runtime initialized"
        );

        try {

            PlatformInitializer initializer =
                    new PlatformInitializer();

            log.debug(() ->
                    "[PlatformInitializer][main] PlatformInitializer instance created"
            );

            initializer.initializePlatform(args);

            initializer.startPlatform();

        } catch (Exception exception) {

            log.error(
                    "[PlatformInitializer] Fatal initialization failure",
                    exception
            );

//            exception.printStackTrace();

            throw new IllegalStateException(
                    "PLATFORM JVM initialization failed",
                    exception
            );
        }
    }
}
