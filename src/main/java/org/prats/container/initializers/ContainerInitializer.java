//
//package org.prats.container.initializers;
//
//import org.prats.database.databaseUtilities.HikariDBConnectionPool.HikariDBConnectionPool;
//import org.prats.database.h2Database.H2DatabaseResourceManager;
//import org.prats.definitions.classes.ProcessContext;
//import org.prats.definitions.enums.ContainerType;
//import org.prats.definitions.enums.ProcessType;
//import org.prats.definitions.enums.TCPConnectionMode;
//import org.prats.definitions.records.ConsolidatedECConfig;
//import org.prats.definitions.records.EC;
//import org.prats.forwarders.CommandForwarder;
//import org.prats.forwarders.DatabaseForwarder;
//import org.prats.handlers.ExecutionContainerCommandHandler;
//import org.prats.handlers.DatabaseRequestHandler;
//import org.prats.parsers.MessageParser;
//import org.prats.parsers.CommandParser;
//import org.prats.parsers.configParser.ECConfigParser;
//import org.prats.processors.*;
//import org.prats.registries.ContainerServiceRegistry;
//import org.prats.registries.CorrelationRegistry;
//import org.prats.registries.RetryRegistry;
//import org.prats.definitions.interfaces.ServiceRegistry;
//import org.prats.utilities.HealthReportAnalyzer;
//import org.prats.services.runtimeService.ExecutionContainerService;
//import org.prats.services.databaseService.DatabaseService;
//import org.prats.services.healthCheckService.HealthService;
//import org.prats.services.messageService.MessageService;
//import org.prats.services.tcpService.TCPService;
//
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.*;
//
//public class ContainerInitializer {
//
//    private static final Set<String> SUPPORTED_ARGUMENTS = Set.of(
//            "tcp-port",
//            "tcp-backlog",
//            "control-port",
//            "core-threads",
//            "max-threads",
//            "idle-timeout",
//            "database-conf",
//            "container-name",
//            "container-type",
//            "container-conf",
//            "platform-host"
//    );
//
//    private ExecutionContainerService executionContainerService;
//
//    private int tcpPort;
//    private int tcpBacklog;
//    private int controlPort;
//
//    private int coreThreads;
//    private int idleTimeout;
//    private int maxThreads;
//
//    private String databaseConfigFile;
//    private String containerConfigPath;
//    private String containerName;
//
//    private ContainerType containerType;
//    private TCPConnectionMode tcpConnectionMode;
//
//    private ServiceRegistry serviceRegistry;
//
//    private boolean initializationAttempted;
//
//
//    private Map<String, String> parseArguments(String[] args) {
//
//        if (args == null) {
//            throw new IllegalArgumentException(
//                    "Container arguments cannot be null"
//            );
//        }
//
//        Map<String, String> parameters = new LinkedHashMap<>();
//
//        for (int i = 0; i < args.length; i++) {
//
//            String argument = args[i];
//
//            if (argument == null || !argument.startsWith("--")) {
//
//                throw new IllegalArgumentException(
//                        "Invalid argument: " + argument
//                );
//            }
//
//            int separator = argument.indexOf('=');
//
//            String key;
//            String value;
//
//            if (separator >= 0) {
//
//                key = argument.substring(2, separator);
//
//                value = argument.substring(separator + 1);
//
//            } else {
//
//                key = argument.substring(2);
//
//                if (i + 1 >= args.length ||
//                        args[i + 1].startsWith("--")) {
//
//                    throw new IllegalArgumentException(
//                            "Missing value for argument: --" + key
//                    );
//                }
//
//                value = args[++i];
//            }
//
//            if (!SUPPORTED_ARGUMENTS.contains(key)) {
//
//                throw new IllegalArgumentException(
//                        "Unknown container argument: --" + key
//                );
//            }
//
//            if (value == null || value.isBlank()) {
//
//                throw new IllegalArgumentException(
//                        "Empty value supplied for argument: --" + key
//                );
//            }
//
//            if (parameters.putIfAbsent(key, value) != null) {
//
//                throw new IllegalArgumentException(
//                        "Duplicate container argument: --" + key
//                );
//            }
//        }
//
//        return parameters;
//    }
//
//
//    private String required(
//            Map<String, String> parameters,
//            String key
//    ) {
//
//        String value = parameters.get(key);
//
//        if (value == null || value.isBlank()) {
//
//            throw new IllegalArgumentException(
//                    "Missing required argument: --" + key
//            );
//        }
//
//        return value.trim();
//    }
//
//
//    private int parseInteger(
//            Map<String, String> parameters,
//            String key
//    ) {
//
//        String value = required(parameters, key);
//
//        try {
//
//            return Integer.parseInt(value);
//
//        } catch (NumberFormatException exception) {
//
//            throw new IllegalArgumentException(
//                    "Invalid integer for --" + key + ": " + value,
//                    exception
//            );
//        }
//    }
//
//
//    private String resolveConfigPath(
//            Map<String, String> parameters,
//            String key
//    ) {
//
//        String value = required(parameters, key);
//
//        Path path;
//
//        try {
//
//            path = Path.of(value)
//                    .toAbsolutePath()
//                    .normalize();
//
//        } catch (RuntimeException exception) {
//
//            throw new IllegalArgumentException(
//                    "Invalid file path supplied for --" + key,
//                    exception
//            );
//        }
//
//        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
//
//            throw new IllegalArgumentException(
//                    "Configuration file does not exist or is not readable: "
//                            + path
//            );
//        }
//
//        return path.toString();
//    }
//
//    private TCPConnectionMode resolveTCPConnectionMode() {
//
//        if (containerType == null ||
//                containerType == ContainerType.UNKNOWN) {
//
//            throw new IllegalArgumentException(
//                    "Cannot resolve TCP connection mode for container type: "
//                            + containerType
//            );
//        }
//
//        if (containerType == ContainerType.PLATFORM) {
//            return TCPConnectionMode.SERVER_ONLY;
//        }
//
//        if (containerType == ContainerType.EC) {
//            return TCPConnectionMode.CLIENT_ONLY;
//        }
//
//        throw new IllegalArgumentException(
//                "Unsupported container type: " + containerType
//        );
//    }
//
//    private void setParametersFromArgs(String[] args) {
//
//        Map<String, String> parameters = parseArguments(args);
//
//        tcpPort         = parseInteger(parameters, "tcp-port");
//        tcpBacklog      = parseInteger(parameters, "tcp-backlog");
//        controlPort     = parseInteger(parameters, "control-port");
//        coreThreads     = parseInteger(parameters, "core-threads");
//        maxThreads      = parseInteger(parameters, "max-threads");
//        idleTimeout     = parseInteger(parameters, "idle-timeout");
//        containerName   = required(parameters, "container-name");
//
//        containerType = parseContainerType(
//                required(parameters, "container-type")
//        );
//
//        databaseConfigFile = resolveConfigPath(
//                parameters,
//                "database-conf"
//        );
//
//        containerConfigPath = resolveConfigPath(
//                parameters,
//                "container-conf"
//        );
//
//        tcpConnectionMode = resolveTCPConnectionMode();
//
//        validateParameters();
//    }
//
//
//    private ContainerType parseContainerType(String value) {
//
//        try {
//
//            return ContainerType.valueOf(
//                    value.trim().toUpperCase(Locale.ROOT)
//            );
//
//        } catch (IllegalArgumentException exception) {
//
//            throw new IllegalArgumentException(
//                    "Unsupported container type: " + value,
//                    exception
//            );
//        }
//    }
//
//    private void validateParameters() {
//
//        if (tcpPort < 1 || tcpPort > 65535) {
//
//            throw new IllegalArgumentException(
//                    "TCP port must be between 1 and 65535"
//            );
//        }
//
//        if (controlPort < 1 || controlPort > 65535) {
//
//            throw new IllegalArgumentException(
//                    "Control port must be between 1 and 65535"
//            );
//        }
//
//        if (tcpBacklog < 1) {
//
//            throw new IllegalArgumentException(
//                    "TCP backlog must be greater than zero"
//            );
//        }
//
//        if (coreThreads < 0) {
//
//            throw new IllegalArgumentException(
//                    "Core threads cannot be negative"
//            );
//        }
//
//        if (maxThreads < 1) {
//
//            throw new IllegalArgumentException(
//                    "Maximum threads must be greater than zero"
//            );
//        }
//
//        if (coreThreads > maxThreads) {
//
//            throw new IllegalArgumentException(
//                    "Core threads cannot exceed maximum threads"
//            );
//        }
//
//        if (idleTimeout < 0) {
//
//            throw new IllegalArgumentException(
//                    "Idle timeout cannot be negative"
//            );
//        }
//
//        if (containerName.isBlank()) {
//
//            throw new IllegalArgumentException(
//                    "Container name cannot be empty"
//            );
//        }
//    }
//
//
//    private void initializeServiceRegistryAndDependencies() {
//
//        EC ec = Objects.requireNonNull(
//                new ECConfigParser()
//                        .initializeECConfigs(containerConfigPath),
//                "EC configuration parser returned null"
//        );
//
//        List<ConsolidatedECConfig> configuredEC = List.copyOf(
//                Objects.requireNonNull(
//                        ec.ecConfigs(),
//                        "EC configuration list cannot be null"
//                )
//        );
//
//        ProcessType processType;
//
//        if (containerType == ContainerType.PLATFORM) {
//
//            processType = ProcessType.PLATFORM;
//
//        } else if (containerType == ContainerType.EC) {
//
//            processType = ProcessType.EC;
//
//        } else {
//
//            throw new IllegalStateException(
//                    "Unsupported container type: " + containerType
//            );
//        }
//
//
//        CorrelationRegistry correlationRegistry =
//                new CorrelationRegistry();
//
//        RetryRegistry retryRegistry =
//                new RetryRegistry(5, 300);
//
//
//        MessageParser messageParser =
//                new MessageParser(processType);
//
//        CommandParser commandParser =
//                new CommandParser(processType);
//
//
//        ResponseProcessor responseProcessor =
//                new ResponseProcessor(
//                        messageParser,
//                        correlationRegistry,
//                        retryRegistry
//                );
//
//        CommandProcessor commandProcessor =
//                new CommandProcessor(
//                        commandParser,
//                        responseProcessor,
//                        Set.of(
//                                "status",
//                                "get health report",
//                                "restart services all",
//                                "shutdown services all",
//                                "kill"
//                        )
//                );
//
//        HikariDBConnectionPool hikariDBConnectionPool =
//                new HikariDBConnectionPool();
//
//        H2DatabaseResourceManager h2DatabaseResourceManager =
//                new H2DatabaseResourceManager(
//                        databaseConfigFile,
//                        hikariDBConnectionPool
//                );
//
//        DatabaseRequestHandler databaseRequestHandler =
//                new DatabaseRequestHandler(
//                        h2DatabaseResourceManager
//                );
//
//        DatabaseService databaseService =
//                new DatabaseService(
//                        coreThreads,
//                        maxThreads,
//                        idleTimeout,
//                        h2DatabaseResourceManager,
//                        databaseRequestHandler
//                );
//
//        DatabaseForwarder databaseForwarder =
//                new DatabaseForwarder(databaseService);
//
//        MessageProcessor messageProcessor =
//                new MessageProcessor(databaseForwarder);
//
//        RequestProcessor requestProcessor =
//                new RequestProcessor(
//                        correlationRegistry,
//                        messageParser,
//                        commandProcessor,
//                        messageProcessor
//                );
//
//        MessageService messageService =
//                new MessageService(
//                        coreThreads,
//                        maxThreads,
//                        idleTimeout,
//                        5,
//                        600,
//                        600,
//                        requestProcessor,
//                        responseProcessor,
//                        correlationRegistry,
//                        retryRegistry
//                );
//
//
//        serviceRegistry = new ContainerServiceRegistry();
//
//
//        HealthService healthService =
//                new HealthService(
//                        serviceRegistry,
//                        100,
//                        75
//                );
//
//        HealthReportAnalyzer healthReportAnalyzer =
//                new HealthReportAnalyzer();
//
//
//        executionContainerService =
//                new ExecutionContainerService(
//                        coreThreads,
//                        maxThreads,
//                        idleTimeout,
//                        tcpPort,
//                        tcpBacklog,
//                        controlPort,
//                        containerName,
//                        databaseConfigFile,
//                        containerType,
//                        configuredEC,
//                        serviceRegistry,
//                        healthService,
//                        healthReportAnalyzer,
//                        ec
//                );
//
//
//        TCPControlProcessor tcpControlProcessor =
//                new TCPControlProcessor(
//                        executionContainerService,
//                        messageParser
//                );
//
//        TCPApplicationProcessor tcpApplicationProcessor =
//                new TCPApplicationProcessor(
//                        messageService,
//                        messageParser
//                );
//
//        TCPService tcpService =
//                new TCPService(
//                        coreThreads,
//                        maxThreads,
//                        idleTimeout,
//                        tcpPort,
//                        tcpBacklog,
//                        containerName,
//                        List.of(),
//                        tcpApplicationProcessor,
//                        TCPConnectionMode.SERVER_AND_PEERS,
//                        tcpControlProcessor
//                );
//
//        TCPService tcpControlService =
//                new TCPService(
//                        coreThreads,
//                        maxThreads,
//                        idleTimeout,
//                        controlPort,
//                        tcpBacklog,
//                        containerName,
//                        List.of(),
//                        tcpApplicationProcessor,
//                        tcpConnectionMode,
//                        tcpControlProcessor
//                );
//
//
//        commandProcessor.setRequestProcessor(requestProcessor);
//
//        databaseService.setMessageDispatcher(messageService);
//
//        CommandForwarder commandForwarder =
//                new CommandForwarder(executionContainerService);
//
//        ExecutionContainerCommandHandler executionContainerCommandHandler =
//                new ExecutionContainerCommandHandler(
//                        executionContainerService,
//                        commandParser
//                );
//
//        messageProcessor.setCommandForwarder(commandForwarder);
//
//        executionContainerService.setCommandHandler(executionContainerCommandHandler);
//
//        executionContainerService.setMessageDispatcher(messageService);
//
//        executionContainerService.setControlChannelCommunicator(tcpControlService);
//
//        healthService.setServiceFailureReporter(
//                executionContainerService
//        );
//
//
//        if (containerType == ContainerType.PLATFORM) {
//
//            serviceRegistry.registerService(databaseService);
//        }
//
//        if (containerType == ContainerType.PLATFORM)
//            serviceRegistry.registerService(databaseService);
//
//        serviceRegistry.registerService(messageService);
//        serviceRegistry.registerService(tcpService);
//        serviceRegistry.registerService(healthService);
//    }
//
//    private void initializeContainer(String[] args) {
//
//        if (initializationAttempted) {
//
//            throw new IllegalStateException(
//                    "ContainerInitializer cannot be initialized more than once"
//            );
//        }
//
//        initializationAttempted = true;
//
//        setParametersFromArgs(args);
//
//        initializeServiceRegistryAndDependencies();
//
//        ProcessContext.initialize(containerName);
//    }
//
//    public void startContainer() {
//        executionContainerService.start();
//    }
//
//    // jvm enters here
//    public static void main(String[] args) {
//
//        ContainerInitializer initializer =
//                new ContainerInitializer();
//
//        initializer.initializeContainer(args);
//
//        initializer.startContainer();
//    }
//}
