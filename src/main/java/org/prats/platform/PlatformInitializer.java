package org.prats.platform;

import org.prats.internalFileHanldingUtilities.configParser.ConfigParser;
import org.prats.definitions.PlatformDefinition;
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
        PlatformDefinition platformDefinition = new ConfigParser().initializePlatformConfigs(platformConfigFile);

        // populating the platform application params with platformDefinition
        platformAppArgs.add("--tcp-port=" + platformDefinition.getTcpPort());
        platformAppArgs.add("--tcp-backlog=" + platformDefinition.getTcpBacklog());
        platformAppArgs.add("--core-threads=" + platformDefinition.getCoreThreads());
        platformAppArgs.add("--max-threads=" + platformDefinition.getMaxThreads());
        platformAppArgs.add("--idle-timeout=" + platformDefinition.getIdleTimeout());
        platformAppArgs.add("--core-idle-timeout=" + platformDefinition.getCoreIdleTimeout());
        platformAppArgs.add("--database-conf="+platformDefinition.getDatabaseConfig());

        // adding any additional params that the user might have passed from the shell
        platformAppArgs.addAll(additionalPlatformAppArgs);

        return new JVMLauncher().launchJvm(
                platformDefinition.getId(),
                platformDefinition.getJvmArgs(),
                platformDefinition.getClassPath(),
                platformDefinition.getMainClass(),
                platformAppArgs
        );
    }

    public Process initializePlatform(List<String> platformAppArgs, String platformConfigFile) throws IOException {
        this.platformConfigFile = platformConfigFile;
        return initializePlatform(platformAppArgs);
    }
}
