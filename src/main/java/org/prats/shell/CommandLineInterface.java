package org.prats.shell;

import org.prats.internalFileHanldingUtilities.configParser.FilePathParser;
import org.prats.database.h2Database.H2DatabaseEngine;
import org.prats.definitions.FilePathDefinition;
import org.prats.database.h2Database.TableInitializer;
import org.prats.internalFileHanldingUtilities.FileHandler;
import org.prats.loggers.ShellLogger;
import org.prats.platform.PlatformInitializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class CommandLineInterface {

    private FileHandler fileHandler;
    private PlatformInitializer platformInitializer;

    public CommandLineInterface() {
        fileHandler = new FileHandler();
        platformInitializer = new PlatformInitializer();
    }

    public void displayWelcomeMessage(FilePathDefinition filePathDefinition) {
        ShellLogger.log(fileHandler.openAndRead(filePathDefinition.getWelcome()));
    }

    public void displayHelpMessage(FilePathDefinition filePathDefinition) {
        ShellLogger.log(fileHandler.openAndRead(filePathDefinition.getHelp()));
    }

    public void displayCommandNotFoundMessage(String command) {
        System.out.println("Command not found: "+command);
    }

    public void displayExitMessage() {
        System.out.println("Exiting...");
    }

    public void createTablesFromConfig(FilePathDefinition filePathDefinition) {
        ShellLogger.log("Creating tables...");
        new TableInitializer().initializeDatabase(filePathDefinition.getDatabaseConf());
    }

    public void startPlatform(FilePathDefinition filePathDefinition) {
        ShellLogger.log("Starting platform...");
        try
        {
            ShellLogger.log("" + platformInitializer.initializePlatform(new ArrayList<>(), filePathDefinition.getPlatformConf()).pid());
            ShellLogger.log("Started platform.");
        }
        catch (IOException ioe)
        {
            ShellLogger.log("Unable to start platform..." + Arrays.toString(ioe.getStackTrace()));
        }
    }

    public void shutdownH2Database() {
        ShellLogger.log("Shutting down H2 database...");
        H2DatabaseEngine.shutdown();
        ShellLogger.log("Killed H2 database...");
    }

    public void launch() {

        FilePathDefinition filePathDefinition = new FilePathParser().initializeFilePaths("/Users/pratyush./Desktop/coding/mediationEngine2/src/main/resources/shell/allFilePath.conf");
        Scanner sc = new Scanner(System.in);
        String command = "";

        displayWelcomeMessage(filePathDefinition);

        while (!command.equals("me kill"))
        {
            command = sc.nextLine();
            switch (command) {
                case "meng help":                   displayHelpMessage(filePathDefinition);
                                                    break;
                case "meng exit":                   displayExitMessage();
                                                    break;
                case  "meng initialize h2db":       createTablesFromConfig(filePathDefinition);
                                                    break;

                case "meng shutdown h2db":          shutdownH2Database();
                                                    break;

                case "meng start platform":         startPlatform(filePathDefinition);
                                                    break;

                default:                            displayCommandNotFoundMessage(command);
                                                    break;
            }
        }
    }
}
