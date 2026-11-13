
package org.prats.parsers.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.definitions.records.Watchdog;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class WatchdogConfigParser {


    public Watchdog initializeWatchdogConfig(String configFilePath) {

        Path configPath = resolveExistingFile(
                configFilePath,
                null,
                "Watchdog configuration"
        );

        Config rootConfig = ConfigFactory
                .parseFile(configPath.toFile())
                .resolve();

        Config watchdog = rootConfig.getConfig("watchdog");

        String watchdogId = watchdog.getString("id");

        String watchdogLabel = watchdog.getString("label");

        String platformHost = watchdog.getString("platform-host");

        int platformControlPort =
                watchdog.getInt("platform-control-port");

        String mainClass = watchdog.getString("main-class");

        String classPath = watchdog.getString("class-path");

        int coreThreads = watchdog.getInt("core-threads");

        int maxThreads = watchdog.getInt("max-threads");

        int idleTimeout = watchdog.getInt("idle-timeout");

        List<String> jvmArgs =
                watchdog.getStringList("jvm-args");


        List<ConsolidatedECConfig> ecConfigs =
                watchdog.getConfigList("ec-configs")
                        .stream()
                        .map(ecConfig -> parseECConfig(
                                ecConfig,
                                platformControlPort,
                                configPath.getParent()
                        ))
                        .toList();

        validateECConfigurations(ecConfigs);

        return new Watchdog(
                watchdogId,
                watchdogLabel,
                platformHost,
                platformControlPort,
                mainClass,
                classPath,
                coreThreads,
                maxThreads,
                idleTimeout,
                jvmArgs,
                ecConfigs
        );
    }


    private ConsolidatedECConfig parseECConfig(
            Config ecConfig,
            int platformControlPort,
            Path baseDirectory
    ) {

        String ecId = ecConfig.getString("id");

        int tcpPort = ecConfig.getInt("tcp-port");

        int startupOrder = ecConfig.getInt("startup-order");

        String ecConfigPath = resolveExistingFile(
                ecConfig.getString("path"),
                baseDirectory,
                "EC configuration for " + ecId
        ).toString();

        return new ConsolidatedECConfig(
                ecId,
                tcpPort,
                platformControlPort,
                startupOrder,
                ecConfigPath
        );
    }



    private void validateECConfigurations(
            List<ConsolidatedECConfig> ecConfigs
    ) {

        Set<String> ecIds = new HashSet<>();

        Set<Integer> tcpPorts = new HashSet<>();

        for (ConsolidatedECConfig ec : ecConfigs) {

            if (ec.ecId() == null || ec.ecId().isBlank()) {

                throw new IllegalArgumentException(
                        "EC ID cannot be empty"
                );
            }

            if (!ecIds.add(ec.ecId())) {

                throw new IllegalArgumentException(
                        "Duplicate EC ID in Watchdog configuration: "
                                + ec.ecId()
                );
            }

            if (ec.tcpPort() < 1 || ec.tcpPort() > 65535) {

                throw new IllegalArgumentException(
                        "Invalid TCP port for EC: " + ec.ecId()
                );
            }

            /*
             * Multiple EC JVMs on the same host cannot
             * bind to the same local TCP port.
             */
            if (!tcpPorts.add(ec.tcpPort())) {

                throw new IllegalArgumentException(
                        "Duplicate TCP port assigned to EC: "
                                + ec.ecId()
                );
            }

            if (ec.startupOrder() < 1) {

                throw new IllegalArgumentException(
                        "Startup order must be greater than zero for EC: "
                                + ec.ecId()
                );
            }
        }
    }


    private Path resolveExistingFile(
            String configuredPath,
            Path baseDirectory,
            String description
    ) {

        if (configuredPath == null || configuredPath.isBlank()) {

            throw new IllegalArgumentException(
                    description + " path cannot be null or empty"
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
