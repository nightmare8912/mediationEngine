package org.prats.internalFileHanldingUtilities.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.FilePath;

import java.io.File;

public class FilePathParser {

    private String configFilePath;

    public FilePathParser() {}

    public FilePathParser(String configFilePath) {
        this.configFilePath = configFilePath;
    }

    public FilePath initializeFilePaths() {

        // declare the variables
        FilePath filePath = new FilePath();

        // open, read and parse the config file
        Config config = ConfigFactory.parseFile(new File(configFilePath));

        // read the file paths
        String welcomePath      = config.getString("filepath.paths.welcome");
        String helpPath         = config.getString("filepath.paths.help");
        String databaseConfPath = config.getString("filepath.paths.database_conf");
        String platformConfPath = config.getString("filepath.paths.platform_conf");

        // set the file paths in the return value
        filePath.setWelcome(welcomePath);
        filePath.setHelp(helpPath);
        filePath.setDatabaseConf(databaseConfPath);
        filePath.setPlatformConf(platformConfPath);

        return filePath;
    }

    public FilePath initializeFilePaths(String configFilePath) {
        // set the config path
        this.configFilePath = configFilePath;
        return initializeFilePaths();
    }
}