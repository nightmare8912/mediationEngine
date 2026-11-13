
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
import org.prats.handlers.ExecutionContainerCommandHandler;

import org.prats.parsers.CommandParser;
import org.prats.parsers.MessageParser;
import org.prats.parsers.configParser.ECConfigParser;

import org.prats.processors.*;

import org.prats.registries.ContainerServiceRegistry;
import org.prats.registries.CorrelationRegistry;
import org.prats.registries.RetryRegistry;

import org.prats.services.databaseService.DatabaseService;
import org.prats.services.healthCheckService.HealthService;
import org.prats.services.messageService.MessageService;
import org.prats.services.runtimeService.ExecutionContainerService;
import org.prats.services.tcpService.NewTCPService;

import org.prats.utilities.ContainerRuntime;
import org.prats.utilities.HealthReportAnalyzer;
import org.prats.loggers.FileLogger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.InvalidPathException;

import java.util.*;

public class ExecutionContainerInitializer {

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


    // ---------------------------------------------------------
    // ARGUMENT PARSING
    // ---------------------------------------------------------

    private Map<String, String> parseArguments(String[] args) {

        if (args == null) {
            throw new IllegalArgumentException(
                    "Execution container arguments cannot be null"
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
                        "Unknown execution container argument: --" + key
                );
            }

            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(
                        "Empty value supplied for argument: --" + key
                );
            }

            if (parameters.putIfAbsent(key, value) != null) {
                throw new IllegalArgumentException(
                        "Duplicate argument: --" + key
                );
            }
        }

        return parameters;
    }


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


    private int parseInteger(
            Map<String, String> parameters,
            String key
    ) {

        String value = required(parameters, key);

        try {

            return Integer.parseInt(value);

        } catch (NumberFormatException exception) {

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

            throw new IllegalArgumentException(
                    "Invalid path supplied for --" + key,
                    exception
            );
        }

        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException(
                    "Configuration file does not exist or is not readable: "
                            + path
            );
        }

        return path.toString();
    }


    // ---------------------------------------------------------
    // INITIALIZATION PARAMETERS
    // ---------------------------------------------------------

    private void setParametersFromArgs(String[] args) {

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
    }


    private ContainerType parseContainerType(String value) {

        try {

            return ContainerType.valueOf(
                    value.trim().toUpperCase(Locale.ROOT)
            );

        } catch (IllegalArgumentException exception) {

            throw new IllegalArgumentException(
                    "Unsupported container type: " + value,
                    exception
            );
        }
    }


    private void validateParameters() {

        if (tcpPort < 1 || tcpPort > 65535) {
            throw new IllegalArgumentException(
                    "TCP port must be between 1 and 65535"
            );
        }

        if (controlPort < 1 || controlPort > 65535) {
            throw new IllegalArgumentException(
                    "Control port must be between 1 and 65535"
            );
        }

        if (tcpPort == controlPort) {
            throw new IllegalArgumentException(
                    "EC TCP port and control port must be different"
            );
        }

        if (tcpBacklog < 1) {
            throw new IllegalArgumentException(
                    "TCP backlog must be greater than zero"
            );
        }

        if (coreThreads < 0) {
            throw new IllegalArgumentException(
                    "Core threads cannot be negative"
            );
        }

        if (maxThreads < 1) {
            throw new IllegalArgumentException(
                    "Maximum threads must be greater than zero"
            );
        }

        if (coreThreads > maxThreads) {
            throw new IllegalArgumentException(
                    "Core threads cannot exceed maximum threads"
            );
        }

        if (idleTimeout < 0) {
            throw new IllegalArgumentException(
                    "Idle timeout cannot be negative"
            );
        }

        if (containerName.isBlank()) {
            throw new IllegalArgumentException(
                    "Execution container name cannot be empty"
            );
        }
    }


    // ---------------------------------------------------------
    // DATABASE CONFIGURATION RESOLUTION
    // ---------------------------------------------------------

    private String resolveDatabaseConfigPath(EC ec) {

        if (!ec.databaseConfigured()) {
            return null;
        }

        String configuredPath = ec.dbConfigPath();

        if (configuredPath == null || configuredPath.isBlank()) {
            throw new IllegalArgumentException(
                    "Database is enabled but dbConfigPath is missing "
                            + "for EC: " + ec.id()
            );
        }

        Path databasePath;

        try {

            databasePath = Path.of(configuredPath);

        } catch (InvalidPathException exception) {

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

        if (!Files.isRegularFile(databasePath) ||
                !Files.isReadable(databasePath)) {

            throw new IllegalArgumentException(
                    "Database configuration file does not exist "
                            + "or is not readable: " + databasePath
            );
        }

        return databasePath.toString();
    }


    // ---------------------------------------------------------
    // SERVICE INITIALIZATION
    // ---------------------------------------------------------

    private void initializeServiceRegistryAndDependencies() {

        ProcessType processType = ProcessType.EC;

        ContainerType containerType = ContainerType.EC;


        // -----------------------------------------------------
        // 1. Parse EC configuration
        // -----------------------------------------------------

        EC ec = Objects.requireNonNull(
                new ECConfigParser()
                        .initializeECConfigs(containerConfigPath),
                "EC configuration parser returned null"
        );

        List<ConsolidatedECConfig> configuredEC = List.copyOf(
                Objects.requireNonNull(
                        ec.ecConfigs(),
                        "EC configuration list cannot be null"
                )
        );

        // Database configuration belongs to the EC definition.
        // It is not a mandatory JVM argument.

        databaseConfigFile = resolveDatabaseConfigPath(ec);


        // -----------------------------------------------------
        // 2. Registries
        // -----------------------------------------------------

        CorrelationRegistry correlationRegistry =
                new CorrelationRegistry();

        RetryRegistry retryRegistry =
                new RetryRegistry(5, 300);

        serviceRegistry = new ContainerServiceRegistry();


        // -----------------------------------------------------
        // 3. Parsers
        // -----------------------------------------------------

        MessageParser messageParser =
                new MessageParser(processType);

        CommandParser commandParser =
                new CommandParser(processType);


        // -----------------------------------------------------
        // 4. Response processing
        // -----------------------------------------------------

        ResponseProcessor responseProcessor =
                new ResponseProcessor(
                        messageParser,
                        correlationRegistry,
                        retryRegistry
                );


        // -----------------------------------------------------
        // 5. Command processing
        // -----------------------------------------------------

        CommandProcessor commandProcessor =
                new CommandProcessor(
                        commandParser,
                        responseProcessor,
                        Set.of(
                                "status",
                                "get health report",
                                "restart services all",
                                "shutdown services all",
                                "kill"
                        )
                );


        // -----------------------------------------------------
        // 6. Optional database infrastructure
        // -----------------------------------------------------

        DatabaseService databaseService = null;
        DatabaseForwarder databaseForwarder = null;

        if (ec.databaseConfigured()) {

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

            databaseService =
                    new DatabaseService(
                            coreThreads,
                            maxThreads,
                            idleTimeout,
                            h2DatabaseResourceManager,
                            databaseRequestHandler
                    );

            databaseForwarder =
                    new DatabaseForwarder(databaseService);
        }


        // -----------------------------------------------------
        // 7. Message processing
        // -----------------------------------------------------

        // databaseForwarder is intentionally null for ECs
        // that do not have database support configured.

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
                        600,
                        600,
                        requestProcessor,
                        responseProcessor,
                        correlationRegistry,
                        retryRegistry
                );

        commandProcessor.setRequestProcessor(requestProcessor);


        // -----------------------------------------------------
        // 8. Service registry and health monitoring
        // -----------------------------------------------------

        HealthService healthService =
                new HealthService(
                        serviceRegistry,
                        100,
                        75
                );

        HealthReportAnalyzer healthReportAnalyzer =
                new HealthReportAnalyzer();


        // -----------------------------------------------------
        // 9. ExecutionContainerService
        // -----------------------------------------------------

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
                        ec
                );


        // -----------------------------------------------------
        // 10. TCP processors
        // -----------------------------------------------------

        List<TCPPeer> tcpPeers =
                new ArrayList<>();

        for (ConsolidatedECConfig ecConfig : configuredEC) {

            tcpPeers.add(
                    new TCPPeer(
                            ecConfig.ecId(),
                            "127.0.0.1",
                            ecConfig.tcpPort()
                    )
            );
        }


        NewTCPControlProcessor newTCPControlProcessor =
                new NewTCPControlProcessor(
                        TCPConnectionMode.CLIENT_ONLY,
                        tcpPeers,
                        messageParser
                );


        NewTCPApplicationProcessor newTCPApplicationProcessor =
                new NewTCPApplicationProcessor(
                        tcpPeers,
                        messageService,
                        messageParser
                );


        // -----------------------------------------------------
        // 11. Main EC TCP service
        // -----------------------------------------------------

        NewTCPService newTCPService =
                new NewTCPService(
                        coreThreads,
                        maxThreads,
                        idleTimeout,
                        tcpPort,
                        tcpBacklog,
                        true,
                        newTCPApplicationProcessor
                );


        // -----------------------------------------------------
        // 12. Control TCP service
        // -----------------------------------------------------

        NewTCPService newTCPControlService =
                new NewTCPService(
                        coreThreads,
                        maxThreads,
                        idleTimeout,
                        controlPort,
                        tcpBacklog,
                        false,
                        newTCPControlProcessor
                );


        // -----------------------------------------------------
        // 13. Dependency injection
        // -----------------------------------------------------

        // Database is optional. Only inject it when initialized.

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
                newTCPControlProcessor
        );

        healthService.setServiceFailureReporter(
                executionContainerService
        );

        newTCPControlProcessor.setTcpService(newTCPControlService);

        newTCPControlProcessor.setControlMessageHandler(
                executionContainerService
        );

        newTCPApplicationProcessor.setTcpService(newTCPService);


        // -----------------------------------------------------
        // 14. Register EC services
        // -----------------------------------------------------

        if (databaseService != null) {
            serviceRegistry.registerService(databaseService);
        }

        serviceRegistry.registerService(messageService);

        serviceRegistry.registerService(
                "application-tcp",
                newTCPService
        );

        serviceRegistry.registerService(
                "control-tcp",
                newTCPControlService
        );

        serviceRegistry.registerService(healthService);


        FileLogger.log(
                "[ExecutionContainerInitializer] "
                        + "EC dependencies initialized: "
                        + containerName
                        + " | Database configured: "
                        + ec.databaseConfigured()
        );
    }


    // ---------------------------------------------------------
    // EC INITIALIZATION
    // ---------------------------------------------------------

    private void initializeExecutionContainer(String[] args) {

        if (initializationAttempted) {
            throw new IllegalStateException(
                    "ExecutionContainerInitializer cannot be initialized twice"
            );
        }

        initializationAttempted = true;

        setParametersFromArgs(args);

        ProcessContext.initialize(containerName);

        initializeServiceRegistryAndDependencies();
    }


    // ---------------------------------------------------------
    // EC STARTUP
    // ---------------------------------------------------------

    public void startExecutionContainer() {

        Objects.requireNonNull(
                executionContainerService,
                "ExecutionContainerService has not been initialized"
        );

        FileLogger.log(
                "[ExecutionContainerInitializer] Starting EC: "
                        + containerName
        );

        executionContainerService.start();

        FileLogger.log(
                "[ExecutionContainerInitializer] EC startup completed: "
                        + containerName
        );
    }


    // ---------------------------------------------------------
    // JVM ENTRY POINT
    // ---------------------------------------------------------

    public static void main(String[] args) {

        ContainerRuntime.initialize();

        try {

            ExecutionContainerInitializer initializer =
                    new ExecutionContainerInitializer();

            initializer.initializeExecutionContainer(args);

            initializer.startExecutionContainer();

        } catch (Exception exception) {

            FileLogger.log(
                    "[ExecutionContainerInitializer] "
                            + "Fatal initialization failure: "
                            + exception
            );

            exception.printStackTrace();

            throw new IllegalStateException(
                    "Execution container initialization failed",
                    exception
            );
        }
    }
}
