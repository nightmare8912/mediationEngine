package org.prats.shell;

import org.prats.database.databaseUtilities.HikariDBConnectionPool.HikariDBConnectionPool;
import org.prats.fileHanldingUtilities.configParser.FilePathParser;
import org.prats.definitions.FilePath;
import org.prats.database.h2Database.H2TableInitializer;
import org.prats.fileHanldingUtilities.FileHandler;
import org.prats.loggers.ShellLogger;
import org.prats.platform.PlatformInitializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class CommandLineInterface {

    private FileHandler fileHandler;
    private PlatformInitializer platformInitializer;
    private final HikariDBConnectionPool dbConnectionPool;

    public CommandLineInterface() {
        fileHandler         = new FileHandler();
        platformInitializer = new PlatformInitializer();
        dbConnectionPool    = new HikariDBConnectionPool();
    }

    public void displayWelcomeMessage(FilePath filePath) {
        ShellLogger.log(fileHandler.openAndRead(filePath.welcome()));
    }

    public void displayHelpMessage(FilePath filePath) {
        ShellLogger.log(fileHandler.openAndRead(filePath.help()));
    }

    public void displayCommandNotFoundMessage(String command) {
        System.out.println("Command not found: "+command);
    }

    public void displayExitMessage() {
        System.out.println("Exiting...");
    }

    public void createTablesFromConfig(FilePath filePath) {
        ShellLogger.log("Creating tables...");
        new H2TableInitializer().initializeDatabase(filePath.databaseConf());
    }

    public void startPlatform(FilePath filePath) {
        ShellLogger.log("Starting platform...");
        try
        {
            ShellLogger.log("" + platformInitializer.initializePlatform(new ArrayList<>(), filePath.platformConf()).pid());
            ShellLogger.log("Started platform.");
        }
        catch (IOException ioe)
        {
            ShellLogger.log("Unable to start platform..." + Arrays.toString(ioe.getStackTrace()));
        }
    }

    public void shutdownPlatform(FilePath filePath) {
//        int tcpPort;
//        ShellLogger.log("Stopping platform...");
//        try
//        {
//            tcpPort = new ConfigParser().initializePlatformConfigs(filePathDefinition.getPlatformConf()).getTcpPort();
//            ShellLogger.log("Stopped platform.");
//        }
//        catch (IOException ioe)
//        {
//            ShellLogger.log("Unable to start platform..." + Arrays.toString(ioe.getStackTrace()));
//        }
    }

    public void shutdownH2Database() {
        ShellLogger.log("Shutting down H2 database...");
        dbConnectionPool.shutdown();
        ShellLogger.log("Killed H2 database...");
    }

    public void launch() {

        FilePath filePath = new FilePathParser().initializeFilePaths("/Users/pratyush./Desktop/coding/mediationEngine2/src/main/resources/shell/allFilePath.conf");
        Scanner sc = new Scanner(System.in);
        String command = "";

        displayWelcomeMessage(filePath);

        while (!command.equals("meng kill")) {
            command = sc.nextLine();

            switch (command) {
                case "meng help" ->
                        displayHelpMessage(filePath);

                case "meng exit" ->
                        displayExitMessage();

                case "meng initialize h2db" ->
                        createTablesFromConfig(filePath);

                case "meng shutdown h2db" ->
                        shutdownH2Database();

                case "meng start platform" ->
                        startPlatform(filePath);

                case "meng shutdown platform" ->
                        shutdownPlatform(filePath);

                default ->
                        displayCommandNotFoundMessage(command);
            }
        }
    }
}
