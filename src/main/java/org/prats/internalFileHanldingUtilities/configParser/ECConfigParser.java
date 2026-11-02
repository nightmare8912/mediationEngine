package org.prats.internalFileHanldingUtilities.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.EC;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ECConfigParser {
    private String configFilePath;

    public ECConfigParser () {}

    public ECConfigParser (String configFilePath) {
        this.configFilePath = configFilePath;
    }

    public EC initializeECConfigs() {
        // declare the variables
        EC ec = new EC();
        List<String> ecjvmArgsDefinitions = new ArrayList<>();

        // open, read and parse the config file
        Config config = ConfigFactory.parseFile(new File(configFilePath));

        // read the tab name and fields
        String ecLabel          = config.getString("ec.label");
        String ecId             = config.getString("ec.id");
        int ecTcpPort           = config.getInt("ec.tcp-port");
        int ecTcpBacklog        = config.getInt("tcp-backlog");
        String ecMainClass      = config.getString("ec.main-class");
        String ecClassPath      = config.getString("ec.class-path");
        int ecCoreThread        = config.getInt("ec.core-threads");
        int ecMaxThread         = config.getInt("ec.max-threads");
        int ecIdleTimeout       = config.getInt("ec.idle-timeout");
        int ecCoreIdleTimeout   = config.getInt("ec.core-idle-timeout");
        List<String> jvmArgs    = config.getStringList("ec.jvm-args");

        // set the fields of the tab and its name in the return value
        ec.setLabel(ecLabel);
        ec.setId(ecId);
        ec.setTcpPort(ecTcpPort);
        ec.setTcpBacklog(ecTcpBacklog);
        ec.setMainClass(ecMainClass);
        ec.setClassPath(ecClassPath);
        ec.setCoreThreads(ecCoreThread);
        ec.setMaxThreads(ecMaxThread);
        ec.setIdleTimeout(ecIdleTimeout);
        ec.setCoreIdleTimeout(ecCoreIdleTimeout);
        ec.setJvmArgs(ecjvmArgsDefinitions);

        return ec;
    }

    public EC initializeECConfigs(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeECConfigs();
    }
}
