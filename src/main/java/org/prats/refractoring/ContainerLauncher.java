package org.prats.refractoring;

import org.prats.definitions.ContainerType;
import org.prats.definitions.Platform;
import org.prats.fileHanldingUtilities.configParser.ConfigParser;
import org.prats.jvmLauncher.JVMLauncher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ContainerLauncher {

    private String containerConfigFile;
    private ContainerType containerType;


    private Process launchContainer(List<String> additionalContainerAppArgs) throws IOException {
        // initialize platform args, this will be autopopulated from the platform.conf file
        List<String> containerAppArgs = new ArrayList<>();

            if (containerType == ContainerType.PLATFORM) {
                // parse the platform config file to java object
                Platform platformDefinition = new ConfigParser().initializePlatformConfigs(containerConfigFile);

                // populating the platform application params with platformDefinition
                containerAppArgs.add("--tcp-port=" + platformDefinition.tcpPort());
                containerAppArgs.add("--tcp-backlog=" + platformDefinition.tcpBacklog());
                containerAppArgs.add("--core-threads=" + platformDefinition.coreThreads());
                containerAppArgs.add("--max-threads=" + platformDefinition.maxThreads());
                containerAppArgs.add("--idle-timeout=" + platformDefinition.idleTimeout());
                containerAppArgs.add("--core-idle-timeout=" + platformDefinition.coreIdleTimeout());
                containerAppArgs.add("--container-name=" + platformDefinition.id());
                containerAppArgs.add("--container-type=" + containerType.name());

                // adding any additional params that the user might have passed from the shell
                containerAppArgs.addAll(additionalContainerAppArgs);

                return new JVMLauncher().launchJvm(
                        platformDefinition.id(),
                        platformDefinition.jvmArgs(),
                        platformDefinition.classPath(),
                        platformDefinition.mainClass(),
                        containerAppArgs
                );
            }

            return null;
    }

    public Process launchContainer(List<String> containerAppArgs, String containerConfigFile, ContainerType containerType) throws IOException {
        this.containerType          = containerType;
        this.containerConfigFile    = containerConfigFile;
        return launchContainer(containerAppArgs);
    }
}
