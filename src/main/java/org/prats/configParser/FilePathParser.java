package org.prats.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.FilePathDefinition;

import java.io.File;

public class FilePathParser {

    private String configFilePath;

    public FilePathParser() {}

    public FilePathParser(String configFilePath) {
        this.configFilePath = configFilePath;
    }

    public FilePathDefinition initializeFilePaths() {

        // declare the variables
        FilePathDefinition filePathDefinition = new FilePathDefinition();

        // open, read and parse the config file
        Config config = ConfigFactory.parseFile(new File(configFilePath));

        // read the file paths
        String welcomePath = config.getString("filepath.paths.welcome");
        String helpPath = config.getString("filepath.paths.help");
        String databaseConfPath = config.getString("filepath.paths.database_conf");

        // set the file paths in the return value
        filePathDefinition.setWelcome(welcomePath);
        filePathDefinition.setHelp(helpPath);
        filePathDefinition.setDatabaseConf(databaseConfPath);

        return filePathDefinition;
    }

    public FilePathDefinition initializeFilePaths(String configFilePath) {

        // set the config path
        this.configFilePath = configFilePath;

        return initializeFilePaths();
    }
}