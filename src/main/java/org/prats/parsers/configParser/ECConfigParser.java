
package org.prats.parsers.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.definitions.records.EC;

import java.io.File;
import java.util.List;

public class ECConfigParser {

    public EC initializeECConfigs(String configFilePath) {

        Config config                           = ConfigFactory.parseFile(new File(configFilePath));
        Config ec                               = config.getConfig("ec");
        String ecLabel                          = ec.getString("label");
        String ecId                             = ec.getString("id");
        int tcpPort                             = ec.getInt("tcp-port");
        int tcpBacklog                          = ec.getInt("tcp-backlog");
        int controlPort                         = ec.getInt("control-port");
        String mainClass                        = ec.getString("main-class");
        String classPath                        = ec.getString("class-path");
        int coreThreads                         = ec.getInt("core-threads");
        int maxThreads                          = ec.getInt("max-threads");
        int idleTimeout                         = ec.getInt("idle-timeout");
        int coreIdleTimeout                     = ec.getInt("core-idle-timeout");
        boolean databaseConfigured              = ec.getBoolean("db-configured");
        String databaseConfig                   = ec.getString("database-conf");

        List<String> jvmArgs                    = ec.getStringList("jvm-args");
        List<ConsolidatedECConfig> ecConfigs    = ec.getConfigList("ec-configs").stream()
                .map(
                        this::parseECConfig
                ).toList();

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

        return new ConsolidatedECConfig(
                ecConfig.getString("id"),
                ecConfig.getInt("tcp-port"),
                ecConfig.getInt("control-port"),
                ecConfig.getInt("startup-order"),
                ecConfig.getString("path")
        );
    }
}
