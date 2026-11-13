
package org.prats.parsers.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.records.ConsolidatedWatchdogConfig;
import org.prats.definitions.records.Platform;
import org.prats.definitions.records.ConsolidatedECConfig;

import java.io.File;
import java.util.List;

public class PlatformConfigParser {

    public Platform initializePlatformConfigs(String configFilePath) {

        Config config = ConfigFactory.parseFile(new File(configFilePath));

        Config platform = config.getConfig("platform");

        String platformLabel = platform.getString("label");
        String platformId = platform.getString("id");
        int tcpPort = platform.getInt("tcp-port");
        int controlPort = platform.getInt("control-port");
        int tcpBacklog = platform.getInt("tcp-backlog");
        String mainClass = platform.getString("main-class");
        String classPath = platform.getString("class-path");
        int coreThreads = platform.getInt("core-threads");
        int maxThreads = platform.getInt("max-threads");
        int idleTimeout = platform.getInt("idle-timeout");
        int coreIdleTimeout = platform.getInt("core-idle-timeout");
        String databaseConfig = platform.getString("database-conf");

        List<String> jvmArgs = platform.getStringList("jvm-args");

        List<ConsolidatedWatchdogConfig> watchdogConfigs =
                platform.getConfigList("watchdog-configs")
                        .stream()
                        .map(this::parseWatchdogConfig)
                        .toList();


        List<ConsolidatedECConfig> ecConfigs = platform.getConfigList("ec-configs")
                .stream()
                .map(this::parseECConfig)
                .toList();

        return new Platform(
                platformId,
                platformLabel,
                tcpPort,
                tcpBacklog,
                controlPort,
                mainClass,
                classPath,
                coreThreads,
                maxThreads,
                idleTimeout,
                coreIdleTimeout,
                jvmArgs,
                watchdogConfigs,
                ecConfigs,
                databaseConfig
        );
    }

    private ConsolidatedWatchdogConfig parseWatchdogConfig(
            Config watchdogConfig
    ) {

        return new ConsolidatedWatchdogConfig(
                watchdogConfig.getString("id"),
                watchdogConfig.getString("label"),
                watchdogConfig.getString("path")
        );
    }


    private ConsolidatedECConfig parseECConfig(Config ecConfig) {

        return new ConsolidatedECConfig(
                ecConfig.getString("id"),
                ecConfig.getInt("tcp-port"),
                ecConfig.getInt("control-port"),
                ecConfig.getInt("startup-order"),
                ecConfig.getString("path")
        );
    }
}
