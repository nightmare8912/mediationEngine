package org.prats.container.initializers;

import org.prats.definitions.classes.ProcessContext;
import org.prats.definitions.enums.ProcessType;
import org.prats.definitions.enums.TCPConnectionMode;
import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.definitions.records.TCPPeer;
import org.prats.definitions.records.Watchdog;

import org.prats.loggers.EngineLogger;

import org.prats.parsers.MessageParser;
import org.prats.parsers.configParser.WatchdogConfigParser;
import org.prats.processors.TCPProtocolProcessor;
import org.prats.services.tcpService.TCPService;
import org.prats.services.watchdogService.WatchdogService;
import org.prats.utilities.ContainerRuntime;

import java.nio.file.Files;
import java.nio.file.Path;

import java.util.*;
import java.util.stream.Collectors;


public class WatchdogInitializer {

    private static final Set<String> SUPPORTED_ARGUMENTS = Set.of(
            "watchdog-conf"
    );

    private static final EngineLogger log =
            EngineLogger.getLogger(WatchdogInitializer.class);

    private Watchdog watchdogDefinition;

    private Path watchdogConfigPath;

    private WatchdogService watchdogService;

    private boolean initializationAttempted;


    // ---------------------------------------------------------
    // ARGUMENT PARSING
    // ---------------------------------------------------------

    private Map<String, String> parseArguments(String[] args) {

        log.debug(() ->
                "[WatchdogInitializer][parseArguments] Parsing WATCHDOG JVM arguments"
                        + " | argumentCount=" + (args == null ? "null" : args.length)
        );

        if (args == null) {

            log.warn(() ->
                    "[WatchdogInitializer][parseArguments] Argument array is null"
            );

            throw new IllegalArgumentException(
                    "Watchdog arguments cannot be null"
            );
        }

        Map<String, String> parameters = new LinkedHashMap<>();

        for (int i = 0; i < args.length; i++) {

            String argument = args[i];

            if (argument == null || !argument.startsWith("--")) {

                int finalI = i;
                log.warn(() ->
                        "[WatchdogInitializer][parseArguments] Invalid argument format"
                                + " | argument=" + argument
                                + " | argumentIndex=" + finalI
                );

                throw new IllegalArgumentException(
                        "Invalid Watchdog argument: " + argument
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

                    int finalI1 = i;
                    log.warn(() ->
                            "[WatchdogInitializer][parseArguments] Argument value is missing"
                                    + " | argument=" + key
                                    + " | argumentIndex=" + finalI1
                    );

                    throw new IllegalArgumentException(
                            "Missing value for argument: --" + key
                    );
                }

                value = args[++i];
            }

            if (!SUPPORTED_ARGUMENTS.contains(key)) {

                log.warn(() ->
                        "[WatchdogInitializer][parseArguments] Unsupported argument supplied"
                                + " | argument=" + key
                );

                throw new IllegalArgumentException(
                        "Unknown Watchdog argument: --" + key
                );
            }

            if (value == null || value.isBlank()) {

                log.warn(() ->
                        "[WatchdogInitializer][parseArguments] Empty argument value supplied"
                                + " | argument=" + key
                );

                throw new IllegalArgumentException(
                        "Empty value supplied for argument: --" + key
                );
            }

            if (parameters.putIfAbsent(key, value) != null) {

                log.warn(() ->
                        "[WatchdogInitializer][parseArguments] Duplicate argument supplied"
                                + " | argument=" + key
                );

                throw new IllegalArgumentException(
                        "Duplicate Watchdog argument: --" + key
                );
            }

            log.debug(() ->
                    "[WatchdogInitializer][parseArguments] Argument accepted"
                            + " | argument=" + key
            );
        }

        log.debug(() ->
                "[WatchdogInitializer][parseArguments] Argument parsing completed"
                        + " | parsedArguments=" + parameters.keySet()
        );

        return parameters;
    }


    private String required(
            Map<String, String> parameters,
            String key
    ) {

        log.debug(() ->
                "[WatchdogInitializer][required] Resolving mandatory argument"
                        + " | argument=" + key
        );

        String value = parameters.get(key);

        if (value == null || value.isBlank()) {

            log.warn(() ->
                    "[WatchdogInitializer][required] Mandatory argument is missing"
                            + " | argument=" + key
            );

            throw new IllegalArgumentException(
                    "Missing required argument: --" + key
            );
        }

        String resolvedValue = value.trim();

        log.debug(() ->
                "[WatchdogInitializer][required] Mandatory argument resolved"
                        + " | argument=" + key
        );

        return resolvedValue;
    }


    // ---------------------------------------------------------
    // CONFIGURATION PATH
    // ---------------------------------------------------------

    private Path resolveConfigPath(String configuredPath) {

        log.debug(() ->
                "[WatchdogInitializer][resolveConfigPath] Resolving WATCHDOG configuration path"
                        + " | configuredPath=" + configuredPath
        );

        Path path;

        try {

            path = Path.of(configuredPath)
                    .toAbsolutePath()
                    .normalize();

        } catch (RuntimeException exception) {

            log.error(
                    "[WatchdogInitializer][resolveConfigPath] Failed to parse WATCHDOG configuration path"
                            + " | configuredPath=" + configuredPath,
                    exception
            );

            throw new IllegalArgumentException(
                    "Invalid Watchdog configuration path: "
                            + configuredPath,
                    exception
            );
        }

        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {

            log.warn(() ->
                    "[WatchdogInitializer][resolveConfigPath] WATCHDOG configuration file is missing or unreadable"
                            + " | resolvedPath=" + path
                            + " | isRegularFile=" + Files.isRegularFile(path)
                            + " | isReadable=" + Files.isReadable(path)
            );

            throw new IllegalArgumentException(
                    "Watchdog configuration does not exist "
                            + "or is not readable: " + path
            );
        }

        log.debug(() ->
                "[WatchdogInitializer][resolveConfigPath] WATCHDOG configuration path resolved"
                        + " | resolvedPath=" + path
        );

        return path;
    }


    // ---------------------------------------------------------
    // WATCHDOG CONFIGURATION
    // ---------------------------------------------------------

    private void initializeWatchdogDefinition(String[] args) {

        log.info(() ->
                "[WatchdogInitializer][initializeWatchdogDefinition] Initializing WATCHDOG configuration"
        );

        Map<String, String> parameters = parseArguments(args);

        watchdogConfigPath = resolveConfigPath(
                required(parameters, "watchdog-conf")
        );

        log.info(() ->
                "[WatchdogInitializer][initializeWatchdogDefinition] Loading WATCHDOG definition"
                        + " | watchdogConfigPath=" + watchdogConfigPath
        );

        watchdogDefinition = Objects.requireNonNull(
                new WatchdogConfigParser()
                        .initializeWatchdogConfig(
                                watchdogConfigPath.toString()
                        ),
                "Watchdog configuration parser returned null"
        );

        log.debug(() ->
                "[WatchdogInitializer][initializeWatchdogDefinition] WATCHDOG definition loaded"
                        + " | watchdogId=" + watchdogDefinition.id()
                        + " | platformHost=" + watchdogDefinition.platformHost()
                        + " | platformControlPort=" + watchdogDefinition.platformControlPort()
                        + " | ecCount=" + watchdogDefinition.ecConfigs().size()
        );

        validateWatchdogDefinition();

        log.info(() ->
                "[WatchdogInitializer] Loaded Watchdog configuration: "
                        + watchdogDefinition.id()
                        + " | configPath=" + watchdogConfigPath
        );
    }


    // ---------------------------------------------------------
    // CONFIGURATION VALIDATION
    // ---------------------------------------------------------

    private void validateWatchdogDefinition() {

        log.debug(() ->
                "[WatchdogInitializer][validateWatchdogDefinition] Validating WATCHDOG configuration"
                        + " | watchdogId=" + watchdogDefinition.id()
        );

        Objects.requireNonNull(
                watchdogDefinition.id(),
                "Watchdog ID cannot be null"
        );

        if (watchdogDefinition.id().isBlank()) {

            log.warn(() ->
                    "[WatchdogInitializer][validateWatchdogDefinition] WATCHDOG ID is empty"
            );

            throw new IllegalArgumentException(
                    "Watchdog ID cannot be empty"
            );
        }

        Objects.requireNonNull(
                watchdogDefinition.platformHost(),
                "Platform host cannot be null"
        );

        if (watchdogDefinition.platformHost().isBlank()) {

            log.warn(() ->
                    "[WatchdogInitializer][validateWatchdogDefinition] Platform host is empty"
                            + " | watchdogId=" + watchdogDefinition.id()
            );

            throw new IllegalArgumentException(
                    "Platform host cannot be empty"
            );
        }

        int platformControlPort =
                watchdogDefinition.platformControlPort();

        if (platformControlPort < 1 || platformControlPort > 65535) {

            log.warn(() ->
                    "[WatchdogInitializer][validateWatchdogDefinition] Platform control port is outside the valid range"
                            + " | platformControlPort=" + platformControlPort
            );

            throw new IllegalArgumentException(
                    "Invalid Platform control port: "
                            + platformControlPort
            );
        }

        if (watchdogDefinition.coreThreads() < 0) {

            log.warn(() ->
                    "[WatchdogInitializer][validateWatchdogDefinition] Core thread count cannot be negative"
                            + " | coreThreads=" + watchdogDefinition.coreThreads()
            );

            throw new IllegalArgumentException(
                    "Core threads cannot be negative"
            );
        }

        if (watchdogDefinition.maxThreads() < 1) {

            log.warn(() ->
                    "[WatchdogInitializer][validateWatchdogDefinition] Maximum thread count must be positive"
                            + " | maxThreads=" + watchdogDefinition.maxThreads()
            );

            throw new IllegalArgumentException(
                    "Maximum threads must be greater than zero"
            );
        }

        if (watchdogDefinition.coreThreads()
                > watchdogDefinition.maxThreads()) {

            log.warn(() ->
                    "[WatchdogInitializer][validateWatchdogDefinition] Core thread count exceeds maximum thread count"
                            + " | coreThreads=" + watchdogDefinition.coreThreads()
                            + " | maxThreads=" + watchdogDefinition.maxThreads()
            );

            throw new IllegalArgumentException(
                    "Core threads cannot exceed maximum threads"
            );
        }

        if (watchdogDefinition.idleTimeout() < 0) {

            log.warn(() ->
                    "[WatchdogInitializer][validateWatchdogDefinition] Idle timeout cannot be negative"
                            + " | idleTimeout=" + watchdogDefinition.idleTimeout()
            );

            throw new IllegalArgumentException(
                    "Idle timeout cannot be negative"
            );
        }

        Objects.requireNonNull(
                watchdogDefinition.ecConfigs(),
                "Watchdog EC configuration list cannot be null"
        );

        log.info(() ->
                "[WatchdogInitializer][validateWatchdogDefinition] WATCHDOG configuration validation completed"
                        + " | watchdogId=" + watchdogDefinition.id()
                        + " | platformHost=" + watchdogDefinition.platformHost()
                        + " | platformControlPort=" + platformControlPort
                        + " | coreThreads=" + watchdogDefinition.coreThreads()
                        + " | maxThreads=" + watchdogDefinition.maxThreads()
                        + " | idleTimeout=" + watchdogDefinition.idleTimeout()
                        + " | ecCount=" + watchdogDefinition.ecConfigs().size()
        );
    }


    // ---------------------------------------------------------
    // WATCHDOG SERVICE INITIALIZATION
    // ---------------------------------------------------------

    private void initializeWatchdogService() {

        log.info(() ->
                "[WatchdogInitializer][initializeWatchdogService] Initializing WATCHDOG services"
                        + " | watchdogId=" + watchdogDefinition.id()
        );

        /*
         * The Watchdog is a distinct process role.
         *
         * ProcessType.WATCHDOG should be added to your
         * ProcessType enum if it does not exist yet.
         */

        MessageParser messageParser =
                new MessageParser(ProcessType.WATCHDOG);

        log.debug(() ->
                "[WatchdogInitializer][initializeWatchdogService] MessageParser initialized"
                        + " | processType=" + ProcessType.WATCHDOG
        );


        // -----------------------------------------------------
        // 1. Watchdog control handler
        // -----------------------------------------------------

        List<ConsolidatedECConfig> ecProperties =
                watchdogDefinition.ecConfigs();

        log.debug(() ->
                "[WatchdogInitializer][initializeWatchdogService] Building EC configuration path map"
                        + " | configuredECCount=" + ecProperties.size()
        );

        Map<String, String> ecConfigPaths = ecProperties.stream()
                .collect(Collectors.toUnmodifiableMap(
                        ConsolidatedECConfig::ecId,
                        ConsolidatedECConfig::ecConfigPath
                ));

        log.info(() ->
                "[WatchdogInitializer][initializeWatchdogService] EC configuration path map created"
                        + " | mappedECCount=" + ecConfigPaths.size()
                        + " | ecIds=" + ecConfigPaths.keySet()
        );

        watchdogService =
                new WatchdogService(
                        messageParser,
                        ecConfigPaths
                );

        log.info(() ->
                "[WatchdogInitializer][initializeWatchdogService] WatchdogService initialized"
                        + " | watchdogId=" + watchdogDefinition.id()
                        + " | managedECCount=" + ecConfigPaths.size()
        );


        // -----------------------------------------------------
        // 2. TCP control processor
        //
        // Incoming Platform commands are sent directly
        // to WatchdogService.
        // -----------------------------------------------------

//        TCPControlProcessor tcpControlProcessor =
//                new TCPControlProcessor(
//                        watchdogService,
//                        messageParser
//                );


        List<TCPPeer> tcpPeers =
                new ArrayList<>();


        log.debug(() ->
                "[WatchdogInitializer][initializeWatchdogService] Initializing WATCHDOG TCP peer definitions"
        );

//        for(ConsolidatedECConfig ecConfig: watchdogDefinition.ecConfigs()) {
//            tcpPeers.add(
//                    new TCPPeer(
//                            ecConfig.ecId(),
//                            "127.0.0.1",
//                            watchdogDefinition.platformControlPort()
//                    )
//            );
//        }

        tcpPeers.add(
                new TCPPeer(
                        "PLATFORM",
                        watchdogDefinition.platformHost(),
                        watchdogDefinition.platformControlPort()
                )
        );

        log.info(() ->
                "[WatchdogInitializer][initializeWatchdogService] PLATFORM TCP peer configured"
                        + " | peerId=PLATFORM"
                        + " | host=" + watchdogDefinition.platformHost()
                        + " | controlPort=" + watchdogDefinition.platformControlPort()
        );

        TCPProtocolProcessor TCPProtocolProcessor
                = new TCPProtocolProcessor(
                TCPConnectionMode.CLIENT_ONLY,
                tcpPeers,
                messageParser
        );

        log.info(() ->
                "[WatchdogInitializer][initializeWatchdogService] TCPControlProcessor initialized"
                        + " | connectionMode=" + TCPConnectionMode.CLIENT_ONLY
                        + " | peerCount=" + tcpPeers.size()
        );


        // -----------------------------------------------------
        // 3. Outbound control connection
        //
        // Watchdog initiates the connection to Platform.
        // The established TCP socket is bidirectional.
        // -----------------------------------------------------

//        tcpControlService =
//                new TCPService(
//                        watchdogDefinition.coreThreads(),
//                        watchdogDefinition.maxThreads(),
//                        watchdogDefinition.idleTimeout(),
//                        watchdogDefinition.platformControlPort(),
//                        watchdogDefinition.id(),
//                        tcpPeers,
//                        TCPConnectionMode.CLIENT_ONLY,
//                        tcpControlProcessor
//                );


        log.info(() ->
                "[WatchdogInitializer][initializeWatchdogService] Initializing outbound control TCP service"
                        + " | watchdogId=" + watchdogDefinition.id()
                        + " | platformHost=" + watchdogDefinition.platformHost()
                        + " | platformControlPort=" + watchdogDefinition.platformControlPort()
                        + " | coreThreads=" + watchdogDefinition.coreThreads()
                        + " | maxThreads=" + watchdogDefinition.maxThreads()
                        + " | idleTimeout=" + watchdogDefinition.idleTimeout()
                        + " | backlog=50"
        );

        TCPService newTCPControlService
                = new TCPService(
                watchdogDefinition.coreThreads(),
                watchdogDefinition.maxThreads(),
                watchdogDefinition.idleTimeout(),
                watchdogDefinition.platformControlPort(),
                50,
                false,
                TCPProtocolProcessor
        );

        log.info(() ->
                "[WatchdogInitializer][initializeWatchdogService] Outbound control TCP service initialized"
                        + " | serviceType=" + newTCPControlService.getClass().getSimpleName()
                        + " | platformControlPort=" + watchdogDefinition.platformControlPort()
        );

        // -----------------------------------------------------
        // 4. Inject control channel
        // -----------------------------------------------------

//        ControlChannelCommunicator communicator =
//                newTCPControlProcessor;

        log.debug(() ->
                "[WatchdogInitializer][initializeWatchdogService] Injecting TCP service into TCPControlProcessor"
        );

        TCPProtocolProcessor.setTcpService(
                newTCPControlService
        );

        log.debug(() ->
                "[WatchdogInitializer][initializeWatchdogService] Injecting WatchdogService as TCP control message handler"
        );

        TCPProtocolProcessor.setControlMessageHandler(
                watchdogService
        );

        log.debug(() ->
                "[WatchdogInitializer][initializeWatchdogService] Injecting control channel communicator into WatchdogService"
        );

        watchdogService.setControlChannelCommunicator(
                TCPProtocolProcessor
        );

        log.debug(() ->
                "[WatchdogInitializer][initializeWatchdogService] Injecting TCP service into WatchdogService"
        );

        watchdogService.setService(
                newTCPControlService
        );

        watchdogService.setService(newTCPControlService);

        log.info(() ->
                "[WatchdogInitializer] Watchdog dependencies initialized. "
                        + "ID: " + watchdogDefinition.id()
                        + ", Platform: "
                        + watchdogDefinition.platformHost()
                        + ":"
                        + watchdogDefinition.platformControlPort()
                        + " | tcpPeerCount=" + tcpPeers.size()
        );

        log.info(() ->
                "[WatchdogInitializer] EC configurations loaded: "
                        + watchdogDefinition.ecConfigs().size()
                        + " | ecIds=" + ecConfigPaths.keySet()
        );
    }


    // ---------------------------------------------------------
    // WATCHDOG INITIALIZATION
    // ---------------------------------------------------------

    private void initializeWatchdog(String[] args) {

        log.info(() ->
                "[WatchdogInitializer][initializeWatchdog] WATCHDOG initialization requested"
                        + " | initializationAttempted=" + initializationAttempted
        );

        if (initializationAttempted) {

            log.warn(() ->
                    "[WatchdogInitializer][initializeWatchdog] Initialization has already been attempted"
            );

            throw new IllegalStateException(
                    "WatchdogInitializer cannot be initialized twice"
            );
        }

        initializationAttempted = true;

        initializeWatchdogDefinition(args);

        log.info(() ->
                "[WatchdogInitializer][initializeWatchdog] Initializing process context"
                        + " | watchdogId=" + watchdogDefinition.id()
        );

        ProcessContext.initialize(watchdogDefinition.id());

        log.debug(() ->
                "[WatchdogInitializer][initializeWatchdog] Process context initialized"
                        + " | watchdogId=" + watchdogDefinition.id()
        );

        initializeWatchdogService();

        log.info(() ->
                "[WatchdogInitializer][initializeWatchdog] WATCHDOG initialization completed"
                        + " | watchdogId=" + watchdogDefinition.id()
        );
    }


    // ---------------------------------------------------------
    // START WATCHDOG
    // ---------------------------------------------------------

    public void startWatchdog() {

        log.info(() ->
                "[WatchdogInitializer][startWatchdog] WATCHDOG startup requested"
                        + " | watchdogServiceInitialized=" + (watchdogService != null)
                        + " | watchdogId=" + (watchdogDefinition == null ? "unavailable" : watchdogDefinition.id())
        );

        Objects.requireNonNull(
                watchdogService,
                "WatchdogService has not been initialized"
        );

        log.info(() ->
                "[WatchdogInitializer] Starting Watchdog: "
                        + watchdogDefinition.id()
        );

        /*
         * WatchdogService.start() starts its injected
         * ControlChannelCommunicator.
         *
         * No EC is launched here.
         * ECs are launched in response to Platform commands.
         */

        log.debug(() ->
                "[WatchdogInitializer][startWatchdog] Invoking WatchdogService.start()"
                        + " | watchdogId=" + watchdogDefinition.id()
        );

        watchdogService.start();


        log.info(() ->
                "[WatchdogInitializer] Watchdog startup initiated."
                        + " | watchdogId=" + watchdogDefinition.id()
        );
    }


    // ---------------------------------------------------------
    // JVM ENTRY POINT
    // ---------------------------------------------------------

    public static void main(String[] args) {

        log.info(() ->
                "[WatchdogInitializer][main] WATCHDOG JVM entry point invoked"
                        + " | argumentCount=" + (args == null ? "null" : args.length)
        );

        ContainerRuntime.initialize();

        log.debug(() ->
                "[WatchdogInitializer][main] Container runtime initialized"
        );

        try {

            WatchdogInitializer initializer =
                    new WatchdogInitializer();

            log.debug(() ->
                    "[WatchdogInitializer][main] WatchdogInitializer instance created"
            );

            initializer.initializeWatchdog(args);

            initializer.startWatchdog();

        } catch (Exception exception) {

            log.error(
                    "[WatchdogInitializer] Fatal initialization failure",
                    exception
            );

            exception.printStackTrace();

            throw new IllegalStateException(
                    "Watchdog JVM initialization failed",
                    exception
            );
        }
    }
}
