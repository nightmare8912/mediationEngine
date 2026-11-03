package org.prats.platform;

import org.prats.fileHanldingUtilities.configParser.ConfigParser;
import org.prats.definitions.Platform;
import org.prats.jvmLauncher.JVMLauncher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PlatformInitializer {
    private String platformConfigFile;
    public PlatformInitializer() {}

    public PlatformInitializer(String platformConfigFile) { this.platformConfigFile = platformConfigFile; }

    public Process initializePlatform(List<String> additionalPlatformAppArgs) throws IOException {
        // initialize platform args, this will be autopopulated from the platform.conf file
        List<String> platformAppArgs = new ArrayList<>();

        // parse the platform config file to java object
        Platform platformDefinition = new ConfigParser().initializePlatformConfigs(platformConfigFile);

        // populating the platform application params with platformDefinition
        platformAppArgs.add("--tcp-port=" + platformDefinition.tcpPort());
        platformAppArgs.add("--tcp-backlog=" + platformDefinition.tcpBacklog());
        platformAppArgs.add("--core-threads=" + platformDefinition.coreThreads());
        platformAppArgs.add("--max-threads=" + platformDefinition.maxThreads());
        platformAppArgs.add("--idle-timeout=" + platformDefinition.idleTimeout());
        platformAppArgs.add("--core-idle-timeout=" + platformDefinition.coreIdleTimeout());
        platformAppArgs.add("--database-conf="+platformDefinition.databaseConfig());

        // adding any additional params that the user might have passed from the shell
        platformAppArgs.addAll(additionalPlatformAppArgs);

        return new JVMLauncher().launchJvm(
                platformDefinition.id(),
                platformDefinition.jvmArgs(),
                platformDefinition.classPath(),
                platformDefinition.mainClass(),
                platformAppArgs
        );
    }

    public Process initializePlatform(List<String> platformAppArgs, String platformConfigFile) throws IOException {
        this.platformConfigFile = platformConfigFile;
        return initializePlatform(platformAppArgs);
    }
}
