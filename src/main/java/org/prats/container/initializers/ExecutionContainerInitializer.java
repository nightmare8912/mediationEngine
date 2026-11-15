
package org.prats.container.initializers;

import org.prats.database.databaseUtilities.HikariDBConnectionPool.HikariDBConnectionPool;
import org.prats.database.h2Database.H2DatabaseResourceManager;

import org.prats.definitions.classes.ProcessContext;
import org.prats.definitions.enums.ContainerType;
import org.prats.definitions.enums.ProcessType;
import org.prats.definitions.enums.TCPConnectionMode;
import org.prats.definitions.interfaces.ServiceRegistry;
import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.definitions.records.EC;
import org.prats.definitions.records.TCPPeer;

import org.prats.forwarders.CommandForwarder;
import org.prats.forwarders.DatabaseForwarder;

import org.prats.handlers.DatabaseRequestHandler;
import org.prats.handlers.ECControlHandler;
import org.prats.handlers.ExecutionContainerCommandHandler;

import org.prats.loggers.EngineLogger;

import org.prats.parsers.CommandParser;
import org.prats.parsers.MessageParser;
import org.prats.parsers.configParser.ECConfigParser;

import org.prats.processors.*;

import org.prats.registries.ContainerServiceRegistry;
import org.prats.registries.CorrelationRegistry;
import org.prats.registries.RetryRegistry;

import org.prats.services.containerServices.ExecutionContainerService;
import org.prats.services.databaseService.DatabaseService;
import org.prats.services.healthCheckService.HealthService;
import org.prats.services.messageService.MessageService;
import org.prats.services.tcpService.TCPService;

import org.prats.utilities.ApplicationPaths;
import org.prats.utilities.ContainerRuntime;
import org.prats.utilities.ECProcessIdentityStore;
import org.prats.utilities.HealthReportAnalyzer;

import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ExecutionContainerInitializer {

    private static final EngineLogger log =
            EngineLogger.getLogger(ExecutionContainerInitializer.class);

    private static final Set<String> SUPPORTED_ARGUMENTS = Set.of(
            "tcp-port",
            "tcp-backlog",
            "control-port",
            "core-threads",
            "max-threads",
            "idle-timeout",
            "container-name",
            "container-type",
            "container-conf"
    );

    private ExecutionContainerService executionContainerService;
    private ServiceRegistry serviceRegistry;

    private int tcpPort;
    private int tcpBacklog;
    private int controlPort;

    private int coreThreads;
    private int maxThreads;
    private int idleTimeout;

    private String databaseConfigFile;
    private String containerConfigPath;
    private String containerName;

    private boolean initializationAttempted;


    // =========================================================
    // 1. ARGUMENT PARSING
    // =========================================================

    private Map<String, String> parseArguments(String[] args) {

        log.debug(() ->
                "[ExecutionContainerInitializer][parseArguments] Parsing EC JVM arguments"
                        + " | argumentCount=" + (args == null ? 0 : args.length)
        );

        if (args == null) {

            log.error(
                    "[ExecutionContainerInitializer][parseArguments] EC arguments cannot be null"
            );

            throw new IllegalArgumentException(
                    "Execution container arguments cannot be null"
            );
        }

        Map<String, String> parameters = new LinkedHashMap<>();

        for (int i = 0; i < args.length; i++) {

            String argument = args[i];

            if (argument == null || !argument.startsWith("--")) {

                log.warn(() ->
                        "[ExecutionContainerInitializer][parseArguments] Invalid argument format"
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

                if (i + 1 >= args.length
                        || args[i + 1].startsWith("--")) {

                    log.warn(() ->
                            "[ExecutionContainerInitializer][parseArguments] Missing argument value"
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
                        "[ExecutionContainerInitializer][parseArguments] Unsupported argument supplied"
                                + " | argument=" + key
                );

                throw new IllegalArgumentException(
                        "Unknown execution container argument: --" + key
                );
            }

            if (value == null || value.isBlank()) {

                log.warn(() ->
                        "[ExecutionContainerInitializer][parseArguments] Empty argument value supplied"
                                + " | argument=" + key
                );

                throw new IllegalArgumentException(
                        "Empty value supplied for argument: --" + key
                );
            }

            if (parameters.putIfAbsent(key, value) != null) {

                log.warn(() ->
                        "[ExecutionContainerInitializer][parseArguments] Duplicate argument supplied"
                                + " | argument=" + key
                );

                throw new IllegalArgumentException(
                        "Duplicate argument: --" + key
                );
            }

            log.debug(() ->
                    "[ExecutionContainerInitializer][parseArguments] Argument parsed"
                            + " | argument=" + key
            );
        }

        log.debug(() ->
                "[ExecutionContainerInitializer][parseArguments] Argument parsing completed"
                        + " | parsedArguments=" + parameters.size()
        );

        return parameters;
    }


    private String required(
            Map<String, String> parameters,
            String key
    ) {

        String value = parameters.get(key);

        if (value == null || value.isBlank()) {

            log.warn(() ->
                    "[ExecutionContainerInitializer][required] Required argument is missing"
                            + " | argument=" + key
            );

            throw new IllegalArgumentException(
                    "Missing required argument: --" + key
            );
        }

        log.debug(() ->
                "[ExecutionContainerInitializer][required] Required argument resolved"
                        + " | argument=" + key
        );

        return value.trim();
    }


    private int parseInteger(
            Map<String, String> parameters,
            String key
    ) {

        String value = required(parameters, key);

        try {

            int parsedValue = Integer.parseInt(value);

            log.debug(() ->
                    "[ExecutionContainerInitializer][parseInteger] Integer argument parsed"
                            + " | argument=" + key
                            + " | value=" + parsedValue
            );

            return parsedValue;

        } catch (NumberFormatException exception) {

            log.error(
                    "[ExecutionContainerInitializer][parseInteger] Invalid integer argument"
                            + " | argument=" + key
                            + " | value=" + value,
                    exception
            );

            throw new IllegalArgumentException(
                    "Invalid integer supplied for --" + key + ": " + value,
                    exception
            );
        }
    }


    private String resolveConfigPath(
            Map<String, String> parameters,
            String key
    ) {

        String value = required(parameters, key);

        Path path;

        try {

            path = Path.of(value)
                    .toAbsolutePath()
                    .normalize();

        } catch (RuntimeException exception) {

            log.error(
                    "[ExecutionContainerInitializer][resolveConfigPath] Invalid configuration path"
                            + " | argument=" + key,
                    exception
            );

            throw new IllegalArgumentException(
                    "Invalid path supplied for --" + key,
                    exception
            );
        }

        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {

            log.error(
                    "[ExecutionContainerInitializer][resolveConfigPath] Configuration file does not exist or is not readable"
                            + " | argument=" + key
                            + " | path=" + path
            );

            throw new IllegalArgumentException(
                    "Configuration file does not exist or is not readable: "
                            + path
            );
        }

        log.debug(() ->
                "[ExecutionContainerInitializer][resolveConfigPath] Configuration path resolved"
                        + " | argument=" + key
                        + " | path=" + path
        );

        return path.toString();
    }


    // =========================================================
    // 2. INITIALIZATION PARAMETERS
    // =========================================================

    private void setParametersFromArgs(String[] args) {

        log.info(
                "[ExecutionContainerInitializer][setParametersFromArgs] Initializing EC parameters"
        );

        Map<String, String> parameters = parseArguments(args);

        tcpPort = parseInteger(parameters, "tcp-port");
        tcpBacklog = parseInteger(parameters, "tcp-backlog");
        controlPort = parseInteger(parameters, "control-port");

        coreThreads = parseInteger(parameters, "core-threads");
        maxThreads = parseInteger(parameters, "max-threads");
        idleTimeout = parseInteger(parameters, "idle-timeout");

        containerName = required(parameters, "container-name");

        ContainerType containerType = parseContainerType(
                required(parameters, "container-type")
        );

        if (containerType != ContainerType.EC) {

            log.error(
                    "[ExecutionContainerInitializer][setParametersFromArgs] Unsupported container type"
                            + " | receivedType=" + containerType
            );

            throw new IllegalArgumentException(
                    "ExecutionContainerInitializer only supports EC. "
                            + "Received: " + containerType
            );
        }

        containerConfigPath = resolveConfigPath(
                parameters,
                "container-conf"
        );

        validateParameters();

        log.info(() ->
                "[ExecutionContainerInitializer][setParametersFromArgs] EC parameters initialized successfully"
                        + " | containerName=" + containerName
                        + " | tcpPort=" + tcpPort
                        + " | controlPort=" + controlPort
                        + " | tcpBacklog=" + tcpBacklog
                        + " | coreThreads=" + coreThreads
                        + " | maxThreads=" + maxThreads
                        + " | idleTimeout=" + idleTimeout
        );
    }


    private ContainerType parseContainerType(String value) {

        try {

            ContainerType containerType = ContainerType.valueOf(
                    value.trim().toUpperCase(Locale.ROOT)
            );

            log.debug(() ->
                    "[ExecutionContainerInitializer][parseContainerType] Container type parsed"
                            + " | containerType=" + containerType
            );

            return containerType;

        } catch (IllegalArgumentException exception) {

            log.error(
                    "[ExecutionContainerInitializer][parseContainerType] Unsupported container type"
                            + " | suppliedValue=" + value,
                    exception
            );

            throw new IllegalArgumentException(
                    "Unsupported container type: " + value,
                    exception
            );
        }
    }


    private void validateParameters() {

        log.debug(
                "[ExecutionContainerInitializer][validateParameters] Validating EC initialization parameters"
        );

        if (tcpPort < 1 || tcpPort > 65535) {

            log.warn(() ->
                    "[ExecutionContainerInitializer][validateParameters] Invalid TCP port"
                            + " | tcpPort=" + tcpPort
            );

            throw new IllegalArgumentException(
                    "TCP port must be between 1 and 65535"
            );
        }

        if (controlPort < 1 || controlPort > 65535) {

            log.warn(() ->
                    "[ExecutionContainerInitializer][validateParameters] Invalid control port"
                            + " | controlPort=" + controlPort
            );

            throw new IllegalArgumentException(
                    "Control port must be between 1 and 65535"
            );
        }

        if (tcpPort == controlPort) {

            log.warn(() ->
                    "[ExecutionContainerInitializer][validateParameters] TCP and control ports cannot be identical"
                            + " | port=" + tcpPort
            );

            throw new IllegalArgumentException(
                    "EC TCP port and control port must be different"
            );
        }

        if (tcpBacklog < 1) {

            log.warn(() ->
                    "[ExecutionContainerInitializer][validateParameters] Invalid TCP backlog"
                            + " | tcpBacklog=" + tcpBacklog
            );

            throw new IllegalArgumentException(
                    "TCP backlog must be greater than zero"
            );
        }

        if (coreThreads < 0) {

            log.warn(() ->
                    "[ExecutionContainerInitializer][validateParameters] Core threads cannot be negative"
                            + " | coreThreads=" + coreThreads
            );

            throw new IllegalArgumentException(
                    "Core threads cannot be negative"
            );
        }

        if (maxThreads < 1) {

            log.warn(() ->
                    "[ExecutionContainerInitializer][validateParameters] Maximum thread count is invalid"
                            + " | maxThreads=" + maxThreads
            );

            throw new IllegalArgumentException(
                    "Maximum threads must be greater than zero"
            );
        }

        if (coreThreads > maxThreads) {

            log.warn(() ->
                    "[ExecutionContainerInitializer][validateParameters] Core threads exceed maximum threads"
                            + " | coreThreads=" + coreThreads
                            + " | maxThreads=" + maxThreads
            );

            throw new IllegalArgumentException(
                    "Core threads cannot exceed maximum threads"
            );
        }

        if (idleTimeout < 0) {

            log.warn(() ->
                    "[ExecutionContainerInitializer][validateParameters] Idle timeout cannot be negative"
                            + " | idleTimeout=" + idleTimeout
            );

            throw new IllegalArgumentException(
                    "Idle timeout cannot be negative"
            );
        }

        if (containerName.isBlank()) {

            log.warn(
                    "[ExecutionContainerInitializer][validateParameters] Container name cannot be empty"
            );

            throw new IllegalArgumentException(
                    "Execution container name cannot be empty"
            );
        }

        log.debug(
                "[ExecutionContainerInitializer][validateParameters] EC initialization parameters validated successfully"
        );
    }


    // =========================================================
    // 3. DATABASE CONFIGURATION RESOLUTION
    // =========================================================

    private String resolveDatabaseConfigPath(EC ec) {

        if (!ec.databaseConfigured()) {

            log.info(() ->
                    "[ExecutionContainerInitializer][resolveDatabaseConfigPath] Database support is not configured for EC"
                            + " | ecId=" + ec.id()
            );

            return null;
        }

        log.debug(() ->
                "[ExecutionContainerInitializer][resolveDatabaseConfigPath] Resolving database configuration path"
                        + " | ecId=" + ec.id()
        );

        String configuredPath = ec.dbConfigPath();

        if (configuredPath == null || configuredPath.isBlank()) {

            log.error(
                    "[ExecutionContainerInitializer][resolveDatabaseConfigPath] Database is enabled but configuration path is missing"
                            + " | ecId=" + ec.id()
            );

            throw new IllegalArgumentException(
                    "Database is enabled but dbConfigPath is missing "
                            + "for EC: " + ec.id()
            );
        }

        Path databasePath;

        try {

            databasePath = Path.of(configuredPath);

        } catch (InvalidPathException exception) {

            log.error(
                    "[ExecutionContainerInitializer][resolveDatabaseConfigPath] Invalid database configuration path"
                            + " | ecId=" + ec.id(),
                    exception
            );

            throw new IllegalArgumentException(
                    "Invalid database configuration path for EC: "
                            + ec.id(),
                    exception
            );
        }

        // Resolve relative paths against the EC configuration directory.
        if (!databasePath.isAbsolute()) {

            Path ecConfigPath = Path.of(containerConfigPath);

            databasePath = ecConfigPath.getParent()
                    .resolve(databasePath);
        }

        databasePath = databasePath
                .toAbsolutePath()
                .normalize();

        final Path resolvedDatabasePath = databasePath;

        log.debug(() ->
                "[ExecutionContainerInitializer][resolveDatabaseConfigPath] Database configuration path resolved"
                        + " | ecId=" + ec.id()
                        + " | databaseConfigPath=" + resolvedDatabasePath
        );

        if (!Files.isRegularFile(databasePath)
                || !Files.isReadable(databasePath)) {

            log.error(
                    "[ExecutionContainerInitializer][resolveDatabaseConfigPath] Database configuration file does not exist or is not readable"
                            + " | ecId=" + ec.id()
                            + " | databaseConfigPath=" + resolvedDatabasePath
            );

            throw new IllegalArgumentException(
                    "Database configuration file does not exist "
                            + "or is not readable: " + databasePath
            );
        }

        log.info(() ->
                "[ExecutionContainerInitializer][resolveDatabaseConfigPath] Database configuration validated"
                        + " | ecId=" + ec.id()
                        + " | databaseConfigPath=" + resolvedDatabasePath
        );

        return databasePath.toString();
    }


    // =========================================================
    // 4. SERVICE REGISTRY AND DEPENDENCY INITIALIZATION
    // =========================================================

    private void initializeServiceRegistryAndDependencies() {

        log.info(() ->
                "[ExecutionContainerInitializer][initializeServiceRegistryAndDependencies] Beginning EC dependency initialization"
                        + " | containerName=" + containerName
        );

        ProcessType processType = ProcessType.EC;
        ContainerType containerType = ContainerType.EC;


        // -----------------------------------------------------
        // 4.1. Parse EC configuration
        // -----------------------------------------------------

        log.debug(
                "[ExecutionContainerInitializer] Parsing EC configuration"
        );

        EC ec = Objects.requireNonNull(
                new ECConfigParser().initializeECConfigs(containerConfigPath),
                "EC configuration parser returned null"
        );

        List<ConsolidatedECConfig> configuredEC = List.copyOf(
                Objects.requireNonNull(
                        ec.ecConfigs(),
                        "EC configuration list cannot be null"
                )
        );

        log.info(() ->
                "[ExecutionContainerInitializer] EC configuration parsed successfully"
                        + " | ecId=" + ec.id()
                        + " | configuredExecutionContainers=" + configuredEC.size()
                        + " | databaseConfigured=" + ec.databaseConfigured()
        );

        // Database configuration belongs to the EC definition.
        // It is not a mandatory JVM argument.
        databaseConfigFile = resolveDatabaseConfigPath(ec);


        // -----------------------------------------------------
        // 4.2. Initialize registries
        // -----------------------------------------------------

        log.debug(
                "[ExecutionContainerInitializer] Initializing registries"
        );

        CorrelationRegistry correlationRegistry =
                new CorrelationRegistry();

        RetryRegistry retryRegistry =
                new RetryRegistry(5, 300);

        serviceRegistry = new ContainerServiceRegistry();

        log.debug(
                "[ExecutionContainerInitializer] Correlation, retry and service registries initialized"
        );


        // -----------------------------------------------------
        // 4.3. Initialize parsers
        // -----------------------------------------------------

        log.debug(
                "[ExecutionContainerInitializer] Initializing parsers"
        );

        MessageParser messageParser =
                new MessageParser(processType);

        CommandParser commandParser =
                new CommandParser(processType);

        log.debug(
                "[ExecutionContainerInitializer] Message and command parsers initialized"
        );


        // -----------------------------------------------------
        // 4.4. Initialize response processing
        // -----------------------------------------------------

        log.debug(
                "[ExecutionContainerInitializer] Initializing response processing"
        );

        ResponseProcessor responseProcessor =
                new ResponseProcessor(
                        messageParser,
                        correlationRegistry,
                        retryRegistry
                );

        log.debug(
                "[ExecutionContainerInitializer] Response processor initialized"
        );


        // -----------------------------------------------------
        // 4.5. Initialize command processing
        // -----------------------------------------------------

        log.debug(
                "[ExecutionContainerInitializer] Initializing command processing"
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

        log.debug(
                "[ExecutionContainerInitializer] Command processor initialized"
        );


        // -----------------------------------------------------
        // 4.6. Initialize optional database infrastructure
        // -----------------------------------------------------

        DatabaseService databaseService = null;
        DatabaseForwarder databaseForwarder = null;

        if (ec.databaseConfigured()) {

            log.info(() ->
                    "[ExecutionContainerInitializer] Initializing optional database infrastructure"
                            + " | ecId=" + ec.id()
            );

            HikariDBConnectionPool hikariDBConnectionPool =
                    new HikariDBConnectionPool();

            H2DatabaseResourceManager h2DatabaseResourceManager =
                    new H2DatabaseResourceManager(
                            databaseConfigFile,
                            hikariDBConnectionPool
                    );

            DatabaseRequestHandler databaseRequestHandler =
                    new DatabaseRequestHandler(
                            h2DatabaseResourceManager
                    );

            databaseService = new DatabaseService(
                    coreThreads,
                    maxThreads,
                    idleTimeout,
                    h2DatabaseResourceManager,
                    databaseRequestHandler
            );

            databaseForwarder =
                    new DatabaseForwarder(databaseService);

            log.info(
                    "[ExecutionContainerInitializer] Database infrastructure initialized"
            );

        } else {

            log.info(
                    "[ExecutionContainerInitializer] Database infrastructure skipped because database support is disabled"
            );
        }


        // -----------------------------------------------------
        // 4.7. Initialize message processing
        // -----------------------------------------------------

        // databaseForwarder is intentionally null for ECs
        // without configured database support.

        log.debug(
                "[ExecutionContainerInitializer] Initializing message processing"
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

        MessageService messageService =
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

        log.info(
                "[ExecutionContainerInitializer] Message processing initialized"
        );


        // -----------------------------------------------------
        // 4.8. Initialize health monitoring
        // -----------------------------------------------------

        log.debug(
                "[ExecutionContainerInitializer] Initializing health monitoring"
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
                "[ExecutionContainerInitializer] Health monitoring dependencies initialized"
        );


        // -----------------------------------------------------
        // 4.9. Initialize ExecutionContainerService
        // -----------------------------------------------------

        log.debug(
                "[ExecutionContainerInitializer] Initializing execution container service"
        );

        ECControlHandler ecControlHandler =
                new ECControlHandler(
                        healthService,
                        messageParser
                );

        executionContainerService =
                new ExecutionContainerService(
                        coreThreads,
                        maxThreads,
                        idleTimeout,
                        tcpPort,
                        tcpBacklog,
                        controlPort,
                        containerName,
                        databaseConfigFile,
                        containerType,
                        configuredEC,
                        serviceRegistry,
                        healthService,
                        healthReportAnalyzer,
                        ecControlHandler
                );

        log.info(
                "[ExecutionContainerInitializer] Execution container service initialized"
        );


        // -----------------------------------------------------
        // 4.10. Initialize TCP processors
        // -----------------------------------------------------

        log.debug(
                "[ExecutionContainerInitializer] Building configured TCP peer list"
        );

        List<TCPPeer> tcpApplicationPeers = new ArrayList<>();

        for (ConsolidatedECConfig ecConfig : configuredEC) {

            tcpApplicationPeers.add(
                    new TCPPeer(
                            ecConfig.ecId(),
                            ecConfig.host(),
                            ecConfig.tcpPort()
                    )
            );
        }

        log.debug(() ->
                "[ExecutionContainerInitializer] TCP application peer list constructed"
                        + " | peerCount=" + tcpApplicationPeers.size()
        );

        List<TCPPeer> controlPeers = List.of(
                new TCPPeer(
                        "PLATFORM",
                        ec.platformHost(),
                        ec.controlPort()
                )
        );

        log.debug(() ->
                "[ExecutionContainerInitializer] TCP control list constructed"
                        + " | peerCount=" + controlPeers.size()
        );

        TCPProtocolProcessor tcpProtocolProcessor =
                new TCPProtocolProcessor(
                        TCPConnectionMode.CLIENT_ONLY,
                        controlPeers,
                        messageParser
                );

        TCPApplicationProcessor tcpApplicationProcessor =
                new TCPApplicationProcessor(
                        tcpApplicationPeers,
                        messageService,
                        messageParser
                );

        log.debug(
                "[ExecutionContainerInitializer] TCP application and control processors initialized"
        );


        // -----------------------------------------------------
        // 4.11. Initialize main EC TCP service
        // -----------------------------------------------------

        log.debug(
                "[ExecutionContainerInitializer] Initializing main EC TCP service"
        );

        TCPService applicationTcpService =
                new TCPService(
                        coreThreads,
                        maxThreads,
                        idleTimeout,
                        ec.tcpBindHost(),
                        tcpPort,
                        tcpBacklog,
                        true,
                        tcpApplicationProcessor
                );

        log.info(() ->
                "[ExecutionContainerInitializer] Main EC TCP service initialized"
                        + " | tcpPort=" + tcpPort
        );


        // -----------------------------------------------------
        // 4.12. Initialize control TCP service
        // -----------------------------------------------------

        log.debug(
                "[ExecutionContainerInitializer] Initializing control TCP service"
        );

        TCPService controlTcpService =
                new TCPService(
                        coreThreads,
                        maxThreads,
                        idleTimeout,
                        ec.tcpBindHost(),
                        controlPort,
                        tcpBacklog,
                        false,
                        tcpProtocolProcessor
                );

        log.info(() ->
                "[ExecutionContainerInitializer] Control TCP service initialized"
                        + " | controlPort=" + controlPort
        );


        // -----------------------------------------------------
        // 4.13. Inject service dependencies
        // -----------------------------------------------------

        log.debug(
                "[ExecutionContainerInitializer] Injecting service dependencies"
        );

        // Database support is optional.
        if (databaseService != null) {

            databaseService.setMessageDispatcher(messageService);
        }

        CommandForwarder commandForwarder =
                new CommandForwarder(executionContainerService);

        ExecutionContainerCommandHandler commandHandler =
                new ExecutionContainerCommandHandler(
                        executionContainerService,
                        commandParser
                );

        messageProcessor.setCommandForwarder(commandForwarder);

        executionContainerService.setCommandHandler(commandHandler);

        executionContainerService.setMessageDispatcher(messageService);

        executionContainerService.setControlChannelCommunicator(
                tcpProtocolProcessor
        );

        ecControlHandler.setControlChannelCommunicator(
                tcpProtocolProcessor
        );



        // -----------------------------------------------------
        // 4.15. Complete remaining dependency injection
        // -----------------------------------------------------

        healthService.setServiceFailureReporter(
                executionContainerService
        );

        tcpProtocolProcessor.setTcpService(
                controlTcpService
        );

        tcpProtocolProcessor.setControlMessageHandler(
                executionContainerService
        );

        tcpApplicationProcessor.setTcpService(
                applicationTcpService
        );

        log.info(
                "[ExecutionContainerInitializer] All EC service dependencies injected successfully"
        );


        // -----------------------------------------------------
        // 4.16. Register EC services
        // -----------------------------------------------------

        log.debug(
                "[ExecutionContainerInitializer] Registering EC services"
        );

        if (databaseService != null) {

            serviceRegistry.registerService(databaseService);
        }

        serviceRegistry.registerService(messageService);

        serviceRegistry.registerService(
                "application-tcp",
                applicationTcpService
        );

        serviceRegistry.registerService(
                "control-tcp",
                controlTcpService
        );

        serviceRegistry.registerService(healthService);

        log.info(
                "[ExecutionContainerInitializer] EC service registration completed"
        );

        log.info(() ->
                "[ExecutionContainerInitializer] EC dependencies initialized"
                        + " | containerName=" + containerName
                        + " | registeredServices=" + serviceRegistry.getSize()
                        + " | databaseConfigured=" + ec.databaseConfigured()
        );
    }


    // =========================================================
    // 5. EC INITIALIZATION
    // =========================================================

    private void initializeExecutionContainer(String[] args) {

        log.info(
                "[ExecutionContainerInitializer][initializeExecutionContainer] EC initialization requested"
        );

        if (initializationAttempted) {

            log.error(
                    "[ExecutionContainerInitializer][initializeExecutionContainer] Initialization rejected because it was already attempted"
            );

            throw new IllegalStateException(
                    "ExecutionContainerInitializer cannot be initialized twice"
            );
        }

        initializationAttempted = true;

        setParametersFromArgs(args);

        log.debug(() ->
                "[ExecutionContainerInitializer][initializeExecutionContainer] Initializing process context"
                        + " | containerName=" + containerName
        );

        ProcessContext.initialize(containerName);

        log.info(() ->
                "[ExecutionContainerInitializer][initializeExecutionContainer] Process context initialized"
                        + " | processName=" + ProcessContext.getProcessName()
        );

        initializeServiceRegistryAndDependencies();

        log.info(() ->
                "[ExecutionContainerInitializer][initializeExecutionContainer] EC initialization completed"
                        + " | containerName=" + containerName
        );
    }


    // =========================================================
    // 6. EC STARTUP
    // =========================================================

    public void startExecutionContainer() {

        log.info(() ->
                "[ExecutionContainerInitializer][startExecutionContainer] EC startup requested"
                        + " | containerName=" + containerName
        );

        Objects.requireNonNull(
                executionContainerService,
                "ExecutionContainerService has not been initialized"
        );

        log.info(() ->
                "[ExecutionContainerInitializer][startExecutionContainer] Starting EC"
                        + " | containerName=" + containerName
        );

        executionContainerService.start();

        log.info(() ->
                "[ExecutionContainerInitializer][startExecutionContainer] EC startup completed"
                        + " | containerName=" + containerName
        );
    }


    // =========================================================
    // 7. JVM ENTRY POINT
    // =========================================================

    public static void main(String[] args) {

        log.info(() ->
                "[ExecutionContainerInitializer][main] Execution container JVM entry point invoked"
                        + " | argumentCount=" + (args == null ? 0 : args.length)
        );

        ContainerRuntime.initialize();

        try {

            // -------------------------------------------------
            // 7.1. Construct initializer
            // -------------------------------------------------

            ExecutionContainerInitializer initializer =
                    new ExecutionContainerInitializer();


            // -------------------------------------------------
            // 7.2. Initialize execution container
            // -------------------------------------------------

            initializer.initializeExecutionContainer(args);


            // -------------------------------------------------
            // 7.3. Start execution container
            // -------------------------------------------------

            initializer.startExecutionContainer();


            // -------------------------------------------------
            // 7.4. Persist EC process identity
            // -------------------------------------------------

            ECProcessIdentityStore identityStore =
                    new ECProcessIdentityStore(
                            ApplicationPaths.getHome()
                    );

            identityStore.write(
                    ProcessContext.getProcessName(),
                    ProcessHandle.current()
            );

            log.info(() ->
                    "[ExecutionContainerInitializer][main] EC process identity persisted"
                            + " | pid=" + ProcessHandle.current().pid()
                            + " | identityFile="
                            + identityStore.path(
                            ProcessContext.getProcessName()
                    )
            );


            // -------------------------------------------------
            // 7.5. Startup completed
            // -------------------------------------------------

            log.info(
                    "[ExecutionContainerInitializer][main] Execution container startup sequence completed"
            );

        } catch (Exception exception) {

            log.error(
                    "[ExecutionContainerInitializer][main] Fatal initialization failure",
                    exception
            );

            exception.printStackTrace();

            throw new IllegalStateException(
                    "Execution container initialization failed",
                    exception
            );
        }
    }
}
