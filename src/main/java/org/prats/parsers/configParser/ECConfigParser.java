
package org.prats.parsers.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.definitions.records.EC;
import org.prats.utilities.ApplicationPaths;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class ECConfigParser {

    public EC initializeECConfigs(String configFilePath) {

        Path configPath = resolveExistingFile(
                configFilePath,
                "EC configuration"
        );

        Config config = ConfigFactory
                .parseFile(configPath.toFile())
                .resolve();

        Config ec = config.getConfig("ec");

        String ecLabel = ec.getString("label");
        String ecId = ec.getString("id");

        int tcpPort = ec.getInt("tcp-port");
        int tcpBacklog = ec.getInt("tcp-backlog");
        int controlPort = ec.getInt("control-port");

        String mainClass = ec.getString("main-class");

        String classPath = resolveExistingFile(
                ec.getString("class-path"),
                "EC classpath"
        ).toString();

        int coreThreads = ec.getInt("core-threads");
        int maxThreads = ec.getInt("max-threads");
        int idleTimeout = ec.getInt("idle-timeout");
        int coreIdleTimeout = ec.getInt("core-idle-timeout");

        boolean databaseConfigured =
                ec.getBoolean("db-configured");

        String databaseConfig = null;

        if (databaseConfigured) {

            if (!ec.hasPath("database-conf")) {
                throw new IllegalArgumentException(
                        "Database is enabled but database-conf is missing "
                                + "for EC: " + ecId
                );
            }

            databaseConfig = resolveExistingFile(
                    ec.getString("database-conf"),
                    "EC database configuration"
            ).toString();
        }

        List<String> jvmArgs =
                ec.getStringList("jvm-args");

        List<ConsolidatedECConfig> ecConfigs =
                ec.getConfigList("ec-configs")
                        .stream()
                        .map(this::parseECConfig)
                        .toList();

        return new EC(
                ecId,
                ecLabel,
                tcpPort,
                tcpBacklog,
                controlPort,
                mainClass,
                classPath,
                coreThreads,
                maxThreads,
                idleTimeout,
                coreIdleTimeout,
                databaseConfigured,
                databaseConfig,
                jvmArgs,
                ecConfigs
        );
    }

    private ConsolidatedECConfig parseECConfig(Config ecConfig) {

        String path = resolveExistingFile(
                ecConfig.getString("path"),
                "EC configuration for " + ecConfig.getString("id")
        ).toString();

        return new ConsolidatedECConfig(
                ecConfig.getString("id"),
                ecConfig.getInt("tcp-port"),
                ecConfig.getInt("control-port"),
                ecConfig.getInt("startup-order"),
                path
        );
    }

    private Path resolveExistingFile(
            String configuredPath,
            String description
    ) {

        Path path = ApplicationPaths.resolve(configuredPath);

        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException(
                    description + " does not exist or is not readable: "
                            + path
            );
        }

        return path;
    }
}
