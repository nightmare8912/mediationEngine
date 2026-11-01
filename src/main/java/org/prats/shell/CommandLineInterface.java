package org.prats.shell;

import org.prats.configParser.FilePathParser;
import org.prats.database.h2Database.H2DatabaseEngine;
import org.prats.definitions.FilePathDefinition;
import org.prats.database.h2Database.TableInitializer;
import org.prats.internalFileHanldingUtilities.FileHandler;

import java.util.Scanner;

public class CommandLineInterface {

    private FileHandler fileHandler;

    public CommandLineInterface() { fileHandler = new FileHandler(); }

    public void displayWelcomeMessage(FilePathDefinition filePathDefinition) {
        System.out.println(fileHandler.openAndRead(filePathDefinition.getWelcome()));
    }

    public void displayHelpMessage(FilePathDefinition filePathDefinition) {
        System.out.println(fileHandler.openAndRead(filePathDefinition.getHelp()));
    }

    public void displayCommandNotFoundMessage(String command) {
        System.out.println("Command not found: "+command);
    }

    public void displayExitMessage() {
        System.out.println("Exiting...");
    }

    public void createTablesFromConfig(FilePathDefinition filePathDefinition) {
        System.out.println("Creating tables...");
        new TableInitializer().initializeDatabase(filePathDefinition.getDatabaseConf());
    }

    public void shutdownH2Database() {
        System.out.println("Shutting down H2 database...");
        H2DatabaseEngine.shutdown();
        System.out.println("Killed H2 database...");
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

                default:                            displayCommandNotFoundMessage(command);
                                                    break;
            }
        }
    }
}
