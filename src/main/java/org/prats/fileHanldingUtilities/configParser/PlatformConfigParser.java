
package org.prats.fileHanldingUtilities.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.Platform;
import org.prats.definitions.PlatformECConfig;

import java.io.File;
import java.util.List;

public class PlatformConfigParser {

    public Platform initializePlatformConfigs(String configFilePath) {

        Config config = ConfigFactory.parseFile(new File(configFilePath));

        Config platform = config.getConfig("platform");

        String platformLabel = platform.getString("label");
        String platformId = platform.getString("id");
        int tcpPort = platform.getInt("tcp-port");
        int tcpBacklog = platform.getInt("tcp-backlog");
        String mainClass = platform.getString("main-class");
        String classPath = platform.getString("class-path");
        int coreThreads = platform.getInt("core-threads");
        int maxThreads = platform.getInt("max-threads");
        int idleTimeout = platform.getInt("idle-timeout");
        int coreIdleTimeout = platform.getInt("core-idle-timeout");
        String databaseConfig = platform.getString("database-conf");

        List<String> jvmArgs = platform.getStringList("jvm-args");

        List<PlatformECConfig> ecConfigs = platform.getConfigList("ec-configs")
                .stream()
                .map(this::parseECConfig)
                .toList();

        return new Platform(
                platformId,
                platformLabel,
                tcpPort,
                tcpBacklog,
                mainClass,
                classPath,
                coreThreads,
                maxThreads,
                idleTimeout,
                coreIdleTimeout,
                jvmArgs,
                ecConfigs,
                databaseConfig
        );
    }

    private PlatformECConfig parseECConfig(Config ecConfig) {

        return new PlatformECConfig(
                ecConfig.getString("id"),
                ecConfig.getString("path")
        );
    }
}
