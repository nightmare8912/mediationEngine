
package org.prats.container.launchers;

import org.prats.definitions.enums.ContainerType;
import org.prats.definitions.records.EC;
import org.prats.definitions.records.Platform;
import org.prats.definitions.records.Watchdog;
import org.prats.jvmLauncher.JVMLauncher;
import org.prats.parsers.configParser.ConfigParser;
import org.prats.parsers.configParser.ECConfigParser;
import org.prats.parsers.configParser.WatchdogConfigParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class ContainerLauncher {

    private final JVMLauncher jvmLauncher;

    public ContainerLauncher() {
        this.jvmLauncher = new JVMLauncher();
    }


    public Process launchPlatform(
            String platformConfigFile,
            List<String> additionalAppArgs
    ) throws IOException {

        Path platformConfigPath = resolveExistingFile(
                platformConfigFile,
                null,
                "Platform configuration"
        );

        Platform platformDefinition =
                new ConfigParser()
                        .initializePlatformConfigs(
                                platformConfigPath.toString()
                        );

        Objects.requireNonNull(
                platformDefinition,
                "Platform configuration could not be initialized"
        );

        String databaseConfig = resolveExistingFile(
                platformDefinition.databaseConfig(),
                platformConfigPath.getParent(),
                "Database configuration"
        ).toString();

        List<String> appArgs = new ArrayList<>();

        addArgument(appArgs, "tcp-port", platformDefinition.tcpPort());
        addArgument(appArgs, "tcp-backlog", platformDefinition.tcpBacklog());
        addArgument(appArgs, "control-port", platformDefinition.controlPort());

        addArgument(appArgs, "core-threads", platformDefinition.coreThreads());
        addArgument(appArgs, "max-threads", platformDefinition.maxThreads());
        addArgument(appArgs, "idle-timeout", platformDefinition.idleTimeout());

        addArgument(appArgs, "database-conf", databaseConfig);

        addArgument(appArgs, "container-name", platformDefinition.id());
        addArgument(appArgs, "container-type", ContainerType.PLATFORM.name());

        addArgument(
                appArgs,
                "container-conf",
                platformConfigPath
        );

        appendAdditionalArguments(appArgs, additionalAppArgs);

        return launchJvm(
                platformDefinition.id(),
                platformDefinition.jvmArgs(),
                platformDefinition.classPath(),
                platformDefinition.mainClass(),
                appArgs
        );
    }


    public Process launchWatchdog(
            String watchdogConfigFile,
            List<String> additionalAppArgs
    ) throws IOException {

        Path watchdogConfigPath = resolveExistingFile(
                watchdogConfigFile,
                null,
                "EC configuration"
        );

        Watchdog watchdogDefinition = new WatchdogConfigParser().
                initializeWatchdogConfig(watchdogConfigFile);


        List<String> appArgs = new ArrayList<>();



        addArgument(
                appArgs,
                "watchdog-conf",
                watchdogConfigPath
        );

        appendAdditionalArguments(appArgs, additionalAppArgs);


        return launchJvm(
                watchdogDefinition.id(),
                watchdogDefinition.jvmArgs(),
                watchdogDefinition.classPath(),
                watchdogDefinition.mainClass(),
                appArgs
        );
    }

    public Process launchEC(
            String ecConfigFile,
            List<String> additionalAppArgs
    ) throws IOException {

        Path ecConfigPath = resolveExistingFile(
                ecConfigFile,
                null,
                "EC configuration"
        );

        EC ecProperties = new ECConfigParser().initializeECConfigs(ecConfigFile);

        List<String> appArgs = new ArrayList<>();

        addArgument(appArgs, "tcp-port", ecProperties.tcpPort());
        addArgument(appArgs, "tcp-backlog", ecProperties.tcpBacklog());

        addArgument(appArgs, "control-port", ecProperties.controlPort());

        addArgument(appArgs, "core-threads", ecProperties.coreThreads());
        addArgument(appArgs, "max-threads", ecProperties.maxThreads());
        addArgument(appArgs, "idle-timeout", ecProperties.idleTimeout());

        addArgument(appArgs, "container-name", ecProperties.id());
        addArgument(appArgs, "container-type", ContainerType.EC.name());

        addArgument(appArgs, "container-conf", ecConfigPath);




        appendAdditionalArguments(appArgs, additionalAppArgs);


        return launchJvm(
                ecProperties.id(),
                ecProperties.jvmArgs(),
                ecProperties.classPath(),
                ecProperties.mainClass(),
                appArgs
        );
    }


    private void validateControlEndpoint(
            String platformHost,
            int controlPort
    ) {

        Objects.requireNonNull(
                platformHost,
                "Platform host cannot be null"
        );

        if (platformHost.isBlank()) {
            throw new IllegalArgumentException(
                    "Platform host cannot be empty"
            );
        }

        if (controlPort < 1 || controlPort > 65535) {
            throw new IllegalArgumentException(
                    "Platform control port must be between 1 and 65535"
            );
        }
    }


    private Process launchJvm(
            String processName,
            List<String> jvmArgs,
            String classPath,
            String mainClass,
            List<String> applicationArgs
    ) throws IOException {

        Objects.requireNonNull(
                processName,
                "Process name cannot be null"
        );

        Objects.requireNonNull(
                jvmArgs,
                "JVM arguments cannot be null"
        );

        Objects.requireNonNull(
                classPath,
                "Classpath cannot be null"
        );

        Objects.requireNonNull(
                mainClass,
                "Main class cannot be null"
        );

        Objects.requireNonNull(
                applicationArgs,
                "Application arguments cannot be null"
        );

        if (processName.isBlank() || mainClass.isBlank()) {
            throw new IllegalArgumentException(
                    "Process name and main class cannot be empty"
            );
        }

        List<String> effectiveJvmArgs = new ArrayList<>(jvmArgs);

        effectiveJvmArgs.removeIf(
                arg -> arg.startsWith("-Dmediation.process.name=")
        );

        effectiveJvmArgs.add(
                "-Dmediation.process.name=" + processName
        );

        return jvmLauncher.launchJvm(
                processName,
                effectiveJvmArgs,
                classPath,
                mainClass,
                applicationArgs
        );
    }


    private void addArgument(
            List<String> arguments,
            String name,
            Object value
    ) {

        Objects.requireNonNull(
                value,
                "Value cannot be null for argument: --" + name
        );

        arguments.add("--" + name + "=" + value);
    }


    private void appendAdditionalArguments(
            List<String> arguments,
            List<String> additionalArguments
    ) {

        if (additionalArguments == null ||
                additionalArguments.isEmpty()) {
            return;
        }

        Set<String> existingKeys = new HashSet<>();

        for (String argument : arguments) {
            existingKeys.add(extractArgumentKey(argument));
        }

        for (int i = 0; i < additionalArguments.size(); i++) {

            String argument = additionalArguments.get(i);

            if (argument == null || !argument.startsWith("--")) {
                throw new IllegalArgumentException(
                        "Invalid additional application argument: "
                                + argument
                );
            }

            String key = extractArgumentKey(argument);

            if (!existingKeys.add(key)) {
                throw new IllegalArgumentException(
                        "Duplicate application argument: --" + key
                );
            }

            int separator = argument.indexOf('=');

            if (separator >= 0) {

                String value = argument.substring(separator + 1);

                if (value.isBlank()) {
                    throw new IllegalArgumentException(
                            "Empty value supplied for --" + key
                    );
                }

                arguments.add(argument);

            } else {

                if (i + 1 >= additionalArguments.size()) {
                    throw new IllegalArgumentException(
                            "Missing value for additional argument: --" + key
                    );
                }

                String value = additionalArguments.get(++i);

                if (value == null ||
                        value.isBlank() ||
                        value.startsWith("--")) {

                    throw new IllegalArgumentException(
                            "Invalid value for additional argument: --" + key
                    );
                }

                arguments.add(argument);
                arguments.add(value);
            }
        }
    }


    private String extractArgumentKey(String argument) {

        if (argument == null || !argument.startsWith("--")) {
            throw new IllegalArgumentException(
                    "Invalid application argument: " + argument
            );
        }

        int separator = argument.indexOf('=');

        String key = separator >= 0
                ? argument.substring(2, separator)
                : argument.substring(2);

        if (key.isBlank()) {
            throw new IllegalArgumentException(
                    "Application argument name cannot be empty"
            );
        }

        return key;
    }


    private Path resolveExistingFile(
            String configuredPath,
            Path baseDirectory,
            String description
    ) {

        Objects.requireNonNull(
                configuredPath,
                description + " path cannot be null"
        );

        if (configuredPath.isBlank()) {
            throw new IllegalArgumentException(
                    description + " path cannot be empty"
            );
        }

        Path path = Path.of(configuredPath);

        if (!path.isAbsolute() && baseDirectory != null) {
            path = baseDirectory.resolve(path);
        }

        path = path.toAbsolutePath().normalize();

        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException(
                    description + " does not exist or is not readable: "
                            + path
            );
        }

        return path;
    }
}
