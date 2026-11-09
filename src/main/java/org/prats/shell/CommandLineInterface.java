package org.prats.shell;

import org.prats.definitions.ContainerType;
import org.prats.fileHanldingUtilities.configParser.FilePathParser;
import org.prats.definitions.FilePath;
import org.prats.fileHanldingUtilities.FileHandler;
import org.prats.loggers.ShellLogger;
import org.prats.refractoring.ContainerLauncher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class CommandLineInterface {

    private final FileHandler fileHandler;

    public CommandLineInterface() {
        fileHandler         = new FileHandler();
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
        System.exit(0);
    }

    public void createTablesFromConfig(FilePath filePath) {
//        ShellLogger.log("Creating tables...");
//        new H2TableInitializer().initializeDatabase(filePath.databaseConf());
    }

    public void startContainer(FilePath filePath, ContainerType containerType) {
        ShellLogger.log("Starting platform...");
        try
        {
            ShellLogger.log("" + new ContainerLauncher().launchContainer(
                    new ArrayList<>(),
                    filePath.platformConf(),
                    containerType
            ).pid());
            ShellLogger.log("Started " + containerType + ".");
        }
        catch (IOException ioe)
        {
            ShellLogger.log("Unable to start: " + containerType + ".\n" + Arrays.toString(ioe.getStackTrace()));
        }
    }


    public void launch() {

        FilePath filePath = new FilePathParser().initializeFilePaths("/Users/pratyush./Desktop/coding/mediationEngine2/src/main/resources/shell/allFilePath.conf");
        Scanner sc = new Scanner(System.in);
        String command = "";

        displayWelcomeMessage(filePath);

        while (!command.equals("meng exit")) {
            command = sc.nextLine();

            switch (command) {
                case "meng help" ->
                        displayHelpMessage(filePath);

                case "meng exit" ->
                        displayExitMessage();

                case "meng start platform" ->
                        startContainer(filePath, ContainerType.PLATFORM);

                default ->
                        displayCommandNotFoundMessage(command);
            }
        }
    }
}
