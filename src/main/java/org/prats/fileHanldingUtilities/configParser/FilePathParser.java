
package org.prats.fileHanldingUtilities.configParser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.prats.definitions.FilePath;

import java.io.File;

public class FilePathParser {

    public FilePath initializeFilePaths(String configFilePath) {

        Config config = ConfigFactory.parseFile(new File(configFilePath));

        return new FilePath(
                config.getString("filepath.paths.welcome"),
                config.getString("filepath.paths.help"),
                config.getString("filepath.paths.database_conf"),
                config.getString("filepath.paths.platform_conf")
        );
    }
}
