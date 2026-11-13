
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
import org.prats.loggers.FileLogger;

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

    private static long HEART_BEAT_TTL = 3600L;

    private Platform platformDefinition;

    private Path platformConfigPath;
    private Path databaseConfigPath;

    private ServiceRegistry serviceRegistry;

    private PlatformService platformService;

    private MessageService messageService;

    private DatabaseService databaseService;


    private boolean initializationAttempted;


    /*
     * Parses application arguments.
     *
     * Supports:
     * --key=value
     * --key value
     */
    private Map<String, String> parseArguments(String[] args) {

        if (args == null) {

            throw new IllegalArgumentException(
                    "Platform arguments cannot be null"
            );
        }

        Map<String, String> parameters = new LinkedHashMap<>();

        for (int i = 0; i < args.length; i++) {

            String argument = args[i];

            if (argument == null || !argument.startsWith("--")) {

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

                    throw new IllegalArgumentException(
                            "Missing value for argument: --" + key
                    );
                }

                value = args[++i];
            }

            if (!SUPPORTED_ARGUMENTS.contains(key)) {

                throw new IllegalArgumentException(
                        "Unknown platform argument: --" + key
                );
            }

            if (value == null || value.isBlank()) {

                throw new IllegalArgumentException(
                        "Empty value supplied for argument: --" + key
                );
            }

            if (parameters.putIfAbsent(key, value) != null) {

                throw new IllegalArgumentException(
                        "Duplicate platform argument: --" + key
                );
            }
        }

        return parameters;
    }


    /*
     * Retrieves a mandatory parameter.
     */
    private String required(
            Map<String, String> parameters,
            String key
    ) {

        String value = parameters.get(key);

        if (value == null || value.isBlank()) {

            throw new IllegalArgumentException(
                    "Missing required argument: --" + key
            );
        }

        return value.trim();
    }


    /*
     * Resolves a configuration path.
     */
    private Path resolveConfigPath(String configuredPath) {

        Path path = ApplicationPaths.resolve(configuredPath);

        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException(
                    "Configuration file does not exist or is not readable: "
                            + path
            );
        }

        return path;
    }


    /*
     * Resolves database configuration relative to the
     * platform configuration file when necessary.
     */
    private Path resolveDatabaseConfigPath(String configuredPath) {

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

        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException(
                    "Database configuration does not exist or is not readable: "
                            + path
            );
        }

        return path;
    }


    /*
     * Reads the platform definition.
     *
     * The platform configuration is the source of truth.
     * Individual JVM arguments are not used to reconstruct
     * the Platform object.
     */
    private void initializeParameters(String[] args) {

        Map<String, String> parameters = parseArguments(args);

        platformConfigPath = resolveConfigPath(
                required(parameters, "container-conf")
        );

        platformDefinition = Objects.requireNonNull(
                new ConfigParser().initializePlatformConfigs(
                        platformConfigPath.toString()
                ),
                "Platform configuration parser returned null"
        );

        if (parameters.containsKey("container-type")) {

            String suppliedType = parameters.get("container-type");

            if (!"PLATFORM".equalsIgnoreCase(suppliedType)) {

                throw new IllegalArgumentException(
                        "PlatformInitializer can only initialize PLATFORM"
                );
            }
        }

        if (parameters.containsKey("container-name")
                && !platformDefinition.id().equals(
                parameters.get("container-name"))) {

            throw new IllegalArgumentException(
                    "Platform name does not match the configuration file"
            );
        }

        databaseConfigPath = resolveDatabaseConfigPath(
                platformDefinition.databaseConfig()
        );

        validatePlatformConfiguration();

        FileLogger.log(
                "[PlatformInitializer] Loaded platform configuration: "
                        + platformDefinition.id()
        );
    }


    /*
     * Validates platform configuration.
     */
    private void validatePlatformConfiguration() {

        if (platformDefinition.id() == null
                || platformDefinition.id().isBlank()) {

            throw new IllegalArgumentException(
                    "Platform ID cannot be empty"
            );
        }

        if (platformDefinition.tcpPort() < 1
                || platformDefinition.tcpPort() > 65535) {

            throw new IllegalArgumentException(
                    "Invalid platform TCP port"
            );
        }

        if (platformDefinition.controlPort() < 1
                || platformDefinition.controlPort() > 65535) {

            throw new IllegalArgumentException(
                    "Invalid platform control port"
            );
        }

        if (platformDefinition.tcpPort()
                == platformDefinition.controlPort()) {

            throw new IllegalArgumentException(
                    "Platform TCP port and control port must be different"
            );
        }

        if (platformDefinition.tcpBacklog() < 1) {

            throw new IllegalArgumentException(
                    "TCP backlog must be greater than zero"
            );
        }

        if (platformDefinition.coreThreads() < 0
                || platformDefinition.maxThreads() < 1
                || platformDefinition.coreThreads()
                > platformDefinition.maxThreads()) {

            throw new IllegalArgumentException(
                    "Invalid platform thread configuration"
            );
        }

        if (platformDefinition.idleTimeout() < 0) {

            throw new IllegalArgumentException(
                    "Idle timeout cannot be negative"
            );
        }
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


        // -------------------------------------------------
        // 1. Registries
        // -------------------------------------------------

        CorrelationRegistry correlationRegistry =
                new CorrelationRegistry();

        RetryRegistry retryRegistry =
                new RetryRegistry(5, 300);

        serviceRegistry = new ContainerServiceRegistry();


        // -------------------------------------------------
        // 2. Parsers
        // -------------------------------------------------

        MessageParser messageParser =
                new MessageParser(processType);

        CommandParser commandParser =
                new CommandParser(processType);


        // -------------------------------------------------
        // 3. Database infrastructure
        // -------------------------------------------------

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
                                "status",
                                "get health report",
                                "start ec",
                                "stop ec",
                                "restart ec",
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
                        600,
                        600,
                        requestProcessor,
                        responseProcessor,
                        correlationRegistry,
                        retryRegistry
                );

        commandProcessor.setRequestProcessor(requestProcessor);


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

        // -------------------------------------------------
        // 7. ECWatchdogMap
        // -------------------------------------------------

        Map<String, ConsolidatedWatchdogConfig> ecWatchdogMap =
                new HashMap<>();

        for (ConsolidatedWatchdogConfig watchdogConfig: platformDefinition.watchdogConfigs()) {

            Watchdog watchdog   = new WatchdogConfigParser().initializeWatchdogConfig(watchdogConfig.configPath());

            List<ConsolidatedECConfig> ecConfigs = watchdog.ecConfigs();

            for (ConsolidatedECConfig ecConfig: ecConfigs) {

                ecWatchdogMap.put(
                        ecConfig.ecId(),
                        watchdogConfig
                );
            }
        }

        // -------------------------------------------------
        // 8. Platform service
        // -------------------------------------------------

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
                        platformControlHandler
                );

        CommandForwarder commandForwarder =
                new CommandForwarder(platformService);

        messageProcessor.setCommandForwarder(commandForwarder);

        platformService.setCommandHandler(
                platformCommandHandler
        );

        platformService.setMessageDispatcher(
                messageService
        );


        // -------------------------------------------------
        // 8. TCP processors
        // -------------------------------------------------

//        TCPApplicationProcessor tcpApplicationProcessor =
//                new TCPApplicationProcessor(
//                        messageService,
//                        messageParser
//                );
//
//        TCPControlProcessor tcpControlProcessor =
//                new TCPControlProcessor(
//                        platformService,
//                        messageParser
//                );


        List<TCPPeer> tcpPeers =
                new ArrayList<>();


        for(ConsolidatedECConfig ecConfig: platformDefinition.ecConfigs()) {
            tcpPeers.add(
                    new TCPPeer(
                            ecConfig.ecId(),
                            "127.0.0.1",
                            ecConfig.tcpPort()
                    )
            );
        }

        for(ConsolidatedWatchdogConfig watchdogConfig: platformDefinition.watchdogConfigs()) {
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


        TCPApplicationProcessor TCPApplicationProcessor
                = new TCPApplicationProcessor(
                tcpPeers,
                messageService,
                messageParser
        );


        // -------------------------------------------------
        // 9. Normal TCP service
        //
        // User/application communication.
        // -------------------------------------------------

//        tcpService =
//                new TCPService(
//                        coreThreads,
//                        maxThreads,
//                        idleTimeout,
//                        tcpPort,
//                        tcpBacklog,
//                        platformName,
//                        tcpPeers,
//                        tcpApplicationProcessor,
//                        TCPConnectionMode.SERVER_AND_PEERS,
//                        tcpControlProcessor
//                );

        TCPService TCPService
                = new TCPService(
                coreThreads,
                maxThreads,
                idleTimeout,
                tcpPort,
                tcpBacklog,
                true,
                TCPApplicationProcessor

        );


        // -------------------------------------------------
        // 10. Control TCP service
        //
        // Watchdog connections.
        // -------------------------------------------------

//        tcpControlService =
//                new TCPService(
//                        coreThreads,
//                        maxThreads,
//                        idleTimeout,
//                        controlPort,
//                        tcpBacklog,
//                        platformName + "-CONTROL",
//                        tcpPeers,
//                        tcpApplicationProcessor,
//                        TCPConnectionMode.SERVER_ONLY,
//                        tcpControlProcessor
//                );

        TCPService newTCPControlService
                = new TCPService(
                coreThreads,
                maxThreads,
                idleTimeout,
                controlPort,
                tcpBacklog,
                true,
                TCPControlProcessor
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

        TCPControlProcessor.setTcpService(newTCPControlService);
        TCPControlProcessor.setControlMessageHandler(platformService);

        TCPApplicationProcessor.setTcpService(TCPService);

        // -------------------------------------------------
        // 12. Service registration
        // -------------------------------------------------

        serviceRegistry.registerService(databaseService);

        serviceRegistry.registerService(messageService);

        serviceRegistry.registerService(
                "application-tcp",
                TCPService
        );

        serviceRegistry.registerService(
                "control-tcp",
                newTCPControlService
        );

        serviceRegistry.registerService(healthService);


        FileLogger.log(
                "[PlatformInitializer] Platform dependencies initialized."
        );
    }


    /*
     * Initializes the PLATFORM JVM.
     */
    private void initializePlatform(String[] args) {

        if (initializationAttempted) {

            throw new IllegalStateException(
                    "PlatformInitializer cannot be initialized more than once"
            );
        }

        initializationAttempted = true;

        initializeParameters(args);

        ProcessContext.initialize(platformDefinition.id());

        initializePlatformServices();
    }


    /*
     * Starts the PLATFORM lifecycle.
     */
    public void startPlatform() {

        Objects.requireNonNull(
                platformService,
                "PlatformService has not been initialized"
        );

        FileLogger.log(
                "[PlatformInitializer] Starting PLATFORM: "
                        + platformDefinition.id()
        );

        platformService.start();

        FileLogger.log(
                "[PlatformInitializer] PLATFORM startup completed."
        );
    }


    /*
     * JVM entry point.
     */
    public static void main(String[] args) {

        ContainerRuntime.initialize();

        try {

            PlatformInitializer initializer =
                    new PlatformInitializer();

            initializer.initializePlatform(args);

            initializer.startPlatform();

        } catch (Exception exception) {

            FileLogger.log(
                    "[PlatformInitializer] Fatal initialization failure: "
                            + exception
            );

//            exception.printStackTrace();

            throw new IllegalStateException(
                    "PLATFORM JVM initialization failed",
                    exception
            );
        }
    }
}
