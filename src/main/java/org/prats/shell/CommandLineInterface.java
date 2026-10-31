package org.prats.shell;

import org.prats.configParser.DatabaseConfigParser;
import org.prats.configParser.FilePathParser;
import org.prats.definitions.FilePathDefinition;
import org.prats.h2Database.TableInitializer;
import org.prats.internalFileHanldingUtilities.FileHandler;

import java.io.IOException;
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

    public void launch() {

        FilePathDefinition filePathDefinition = new FilePathParser().initializeFilePaths("/Users/pratyush./Desktop/coding/mediationEngine2/src/main/resources/shell/allFilePath.conf");
        Scanner sc = new Scanner(System.in);
        String command = "";

        displayWelcomeMessage(filePathDefinition);

        while (!command.equals("me kill"))
        {
            command = sc.nextLine();
            switch (command) {
                case "meng help":               displayHelpMessage(filePathDefinition);
                                                break;
                case "meng exit":               displayExitMessage();
                                                break;
                case  "meng create tables":     createTablesFromConfig(filePathDefinition);
                                                break;

                default:                        displayCommandNotFoundMessage(command);
                                                break;
            }
        }
    }
}
