package org.prats.internalFileHanldingUtilities.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.Platform;
import org.prats.definitions.PlatformECConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class PlatformConfigParser {
    private String configFilePath;

    public PlatformConfigParser () {}

    public PlatformConfigParser (String configFilePath) {
        this.configFilePath = configFilePath;
    }

    public Platform initializePlatformConfigs() {
        // declare the variables
        Platform platformDefinition      = new Platform();
        List<PlatformECConfig> platformECConfigs = new ArrayList<>();

        // open, read and parse the config file
        Config config = ConfigFactory.parseFile(new File(configFilePath));

        // read the tab name and fields
        String platformLabel            = config.getString("platform.label");
        String platformLabelId          = config.getString("platform.id");
        int platformTcpPort             = config.getInt("platform.tcp-port");
        int platformTcpBacklog          = config.getInt("platform.tcp-backlog");
        String platformMainClas         = config.getString("platform.main-class");
        String platformClassPath        = config.getString("platform.class-path");
        int platformCoreThread          = config.getInt("platform.core-threads");
        int platformMaxThread           = config.getInt("platform.max-threads");
        int platformIdleTimeout         = config.getInt("platform.idle-timeout");
        int platformCoreIdleTimeout     = config.getInt("platform.core-idle-timeout");
        String platformDatabaseConf     = config.getString("platform.database-conf");
        List<String> jvmArgs            = config.getStringList("platform.jvm-args");
        var ecConfigList                = config.getConfigList("platform.ec-configs");

        for (Config ecConfig : ecConfigList) {

            PlatformECConfig definition =
                    new PlatformECConfig();

            definition.setEcLabel(ecConfig.getString("id"));
            definition.setEcConfigPath(ecConfig.getString("path"));

            platformECConfigs.add(definition);
        }

        // set the fields of the tab and its name in the return value
        platformDefinition.setLabel(platformLabel);
        platformDefinition.setId(platformLabelId);
        platformDefinition.setTcpPort(platformTcpPort);
        platformDefinition.setTcpBacklog(platformTcpBacklog);
        platformDefinition.setMainClass(platformMainClas);
        platformDefinition.setClassPath(platformClassPath);
        platformDefinition.setCoreThreads(platformCoreThread);
        platformDefinition.setMaxThreads(platformMaxThread);
        platformDefinition.setIdleTimeout(platformIdleTimeout);
        platformDefinition.setCoreIdleTimeout(platformCoreIdleTimeout);
        platformDefinition.setDatabaseConfig(platformDatabaseConf);
        platformDefinition.setJvmArgs(jvmArgs);
        platformDefinition.setEcConfigs(platformECConfigs);

        return platformDefinition;
    }

    public Platform initializePlatformConfigs(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializePlatformConfigs();
    }
}
