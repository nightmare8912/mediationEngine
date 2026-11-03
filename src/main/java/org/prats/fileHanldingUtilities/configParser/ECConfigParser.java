
package org.prats.fileHanldingUtilities.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.EC;

import java.io.File;
import java.util.List;

public class ECConfigParser {

    public EC initializeECConfigs(String configFilePath) {

        Config config = ConfigFactory.parseFile(new File(configFilePath));

        Config ec = config.getConfig("ec");

        String ecLabel = ec.getString("label");
        String ecId = ec.getString("id");
        int tcpPort = ec.getInt("tcp-port");
        int tcpBacklog = ec.getInt("tcp-backlog");
        String mainClass = ec.getString("main-class");
        String classPath = ec.getString("class-path");
        int coreThreads = ec.getInt("core-threads");
        int maxThreads = ec.getInt("max-threads");
        int idleTimeout = ec.getInt("idle-timeout");
        int coreIdleTimeout = ec.getInt("core-idle-timeout");

        List<String> jvmArgs = ec.getStringList("jvm-args");

        return new EC(
                ecId,
                ecLabel,
                tcpPort,
                tcpBacklog,
                mainClass,
                classPath,
                coreThreads,
                maxThreads,
                idleTimeout,
                coreIdleTimeout,
                jvmArgs
        );
    }
}
