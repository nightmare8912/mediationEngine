
package org.prats.container.initializers;

import org.prats.definitions.classes.ProcessContext;
import org.prats.definitions.enums.ProcessType;
import org.prats.definitions.enums.TCPConnectionMode;
import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.definitions.records.TCPPeer;
import org.prats.definitions.records.Watchdog;

import org.prats.jvmLauncher.JVMLauncher;

import org.prats.loggers.FileLogger;

import org.prats.parsers.MessageParser;
import org.prats.parsers.configParser.WatchdogConfigParser;
import org.prats.processors.TCPControlProcessor;
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

    private Watchdog watchdogDefinition;

    private Path watchdogConfigPath;

    private WatchdogService watchdogService;

    private boolean initializationAttempted;


    // ---------------------------------------------------------
    // ARGUMENT PARSING
    // ---------------------------------------------------------

    private Map<String, String> parseArguments(String[] args) {

        if (args == null) {

            throw new IllegalArgumentException(
                    "Watchdog arguments cannot be null"
            );
        }

        Map<String, String> parameters = new LinkedHashMap<>();

        for (int i = 0; i < args.length; i++) {

            String argument = args[i];

            if (argument == null || !argument.startsWith("--")) {

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

                    throw new IllegalArgumentException(
                            "Missing value for argument: --" + key
                    );
                }

                value = args[++i];
            }

            if (!SUPPORTED_ARGUMENTS.contains(key)) {

                throw new IllegalArgumentException(
                        "Unknown Watchdog argument: --" + key
                );
            }

            if (value == null || value.isBlank()) {

                throw new IllegalArgumentException(
                        "Empty value supplied for argument: --" + key
                );
            }

            if (parameters.putIfAbsent(key, value) != null) {

                throw new IllegalArgumentException(
                        "Duplicate Watchdog argument: --" + key
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


    // ---------------------------------------------------------
    // CONFIGURATION PATH
    // ---------------------------------------------------------

    private Path resolveConfigPath(String configuredPath) {

        Path path;

        try {

            path = Path.of(configuredPath)
                    .toAbsolutePath()
                    .normalize();

        } catch (RuntimeException exception) {

            throw new IllegalArgumentException(
                    "Invalid Watchdog configuration path: "
                            + configuredPath,
                    exception
            );
        }

        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {

            throw new IllegalArgumentException(
                    "Watchdog configuration does not exist "
                            + "or is not readable: " + path
            );
        }

        return path;
    }


    // ---------------------------------------------------------
    // WATCHDOG CONFIGURATION
    // ---------------------------------------------------------

    private void initializeWatchdogDefinition(String[] args) {

        Map<String, String> parameters = parseArguments(args);

        watchdogConfigPath = resolveConfigPath(
                required(parameters, "watchdog-conf")
        );

        watchdogDefinition = Objects.requireNonNull(
                new WatchdogConfigParser()
                        .initializeWatchdogConfig(
                                watchdogConfigPath.toString()
                        ),
                "Watchdog configuration parser returned null"
        );

        validateWatchdogDefinition();

        FileLogger.log(
                "[WatchdogInitializer] Loaded Watchdog configuration: "
                        + watchdogDefinition.id()
        );
    }


    // ---------------------------------------------------------
    // CONFIGURATION VALIDATION
    // ---------------------------------------------------------

    private void validateWatchdogDefinition() {

        Objects.requireNonNull(
                watchdogDefinition.id(),
                "Watchdog ID cannot be null"
        );

        if (watchdogDefinition.id().isBlank()) {

            throw new IllegalArgumentException(
                    "Watchdog ID cannot be empty"
            );
        }

        Objects.requireNonNull(
                watchdogDefinition.platformHost(),
                "Platform host cannot be null"
        );

        if (watchdogDefinition.platformHost().isBlank()) {

            throw new IllegalArgumentException(
                    "Platform host cannot be empty"
            );
        }

        int platformControlPort =
                watchdogDefinition.platformControlPort();

        if (platformControlPort < 1 || platformControlPort > 65535) {

            throw new IllegalArgumentException(
                    "Invalid Platform control port: "
                            + platformControlPort
            );
        }

        if (watchdogDefinition.coreThreads() < 0) {

            throw new IllegalArgumentException(
                    "Core threads cannot be negative"
            );
        }

        if (watchdogDefinition.maxThreads() < 1) {

            throw new IllegalArgumentException(
                    "Maximum threads must be greater than zero"
            );
        }

        if (watchdogDefinition.coreThreads()
                > watchdogDefinition.maxThreads()) {

            throw new IllegalArgumentException(
                    "Core threads cannot exceed maximum threads"
            );
        }

        if (watchdogDefinition.idleTimeout() < 0) {

            throw new IllegalArgumentException(
                    "Idle timeout cannot be negative"
            );
        }

        Objects.requireNonNull(
                watchdogDefinition.ecConfigs(),
                "Watchdog EC configuration list cannot be null"
        );
    }


    // ---------------------------------------------------------
    // WATCHDOG SERVICE INITIALIZATION
    // ---------------------------------------------------------

    private void initializeWatchdogService() {

        /*
         * The Watchdog is a distinct process role.
         *
         * ProcessType.WATCHDOG should be added to your
         * ProcessType enum if it does not exist yet.
         */

        MessageParser messageParser =
                new MessageParser(ProcessType.WATCHDOG);


        // -----------------------------------------------------
        // 1. Watchdog control handler
        // -----------------------------------------------------

        List<ConsolidatedECConfig> ecProperties =
                watchdogDefinition.ecConfigs();

        Map<String, String> ecConfigPaths = ecProperties.stream()
                .collect(Collectors.toUnmodifiableMap(
                        ConsolidatedECConfig::ecId,
                        ConsolidatedECConfig::ecConfigPath
                ));

        watchdogService =
                new WatchdogService(
                        messageParser,
                        ecConfigPaths
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

        TCPControlProcessor TCPControlProcessor
                = new TCPControlProcessor(
                TCPConnectionMode.CLIENT_ONLY,
                tcpPeers,
                messageParser
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


        TCPService newTCPControlService
                = new TCPService(
                watchdogDefinition.coreThreads(),
                watchdogDefinition.maxThreads(),
                watchdogDefinition.idleTimeout(),
                watchdogDefinition.platformControlPort(),
                50,
                false,
                TCPControlProcessor
        );

        // -----------------------------------------------------
        // 4. Inject control channel
        // -----------------------------------------------------

//        ControlChannelCommunicator communicator =
//                newTCPControlProcessor;

        TCPControlProcessor.setTcpService(
                newTCPControlService
        );

        TCPControlProcessor.setControlMessageHandler(
                watchdogService
        );

        watchdogService.setControlChannelCommunicator(
                TCPControlProcessor
        );

        watchdogService.setService(
                newTCPControlService
        );

        watchdogService.setService(newTCPControlService);

        FileLogger.log(
                "[WatchdogInitializer] Watchdog dependencies initialized. "
                        + "ID: " + watchdogDefinition.id()
                        + ", Platform: "
                        + watchdogDefinition.platformHost()
                        + ":"
                        + watchdogDefinition.platformControlPort()
        );

        FileLogger.log(
                "[WatchdogInitializer] EC configurations loaded: "
                        + watchdogDefinition.ecConfigs().size()
        );
    }


    // ---------------------------------------------------------
    // WATCHDOG INITIALIZATION
    // ---------------------------------------------------------

    private void initializeWatchdog(String[] args) {

        if (initializationAttempted) {

            throw new IllegalStateException(
                    "WatchdogInitializer cannot be initialized twice"
            );
        }

        initializationAttempted = true;

        initializeWatchdogDefinition(args);

        ProcessContext.initialize(watchdogDefinition.id());

        initializeWatchdogService();
    }


    // ---------------------------------------------------------
    // START WATCHDOG
    // ---------------------------------------------------------

    public void startWatchdog() {

        Objects.requireNonNull(
                watchdogService,
                "WatchdogService has not been initialized"
        );

        FileLogger.log(
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

        watchdogService.start();


        FileLogger.log(
                "[WatchdogInitializer] Watchdog startup initiated."
        );
    }


    // ---------------------------------------------------------
    // JVM ENTRY POINT
    // ---------------------------------------------------------

    public static void main(String[] args) {

        ContainerRuntime.initialize();

        try {

            WatchdogInitializer initializer =
                    new WatchdogInitializer();

            initializer.initializeWatchdog(args);

            initializer.startWatchdog();

        } catch (Exception exception) {

            FileLogger.log(
                    "[WatchdogInitializer] Fatal initialization failure: "
                            + exception
            );

            exception.printStackTrace();

            throw new IllegalStateException(
                    "Watchdog JVM initialization failed",
                    exception
            );
        }
    }
}
