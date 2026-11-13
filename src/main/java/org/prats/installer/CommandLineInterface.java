
package org.prats.installer;

import org.prats.definitions.records.FilePath;
import org.prats.container.launchers.ContainerLauncher;
import org.prats.handlers.FileHandler;
import org.prats.loggers.ShellLogger;
import org.prats.parsers.configParser.FilePathParser;
import org.prats.utilities.ApplicationPaths;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;

public class CommandLineInterface {

    private static final String DEFAULT_PATH_CONFIG =
            "conf/installer/allFilePath.conf";

    private final FileHandler fileHandler;
    private final ContainerLauncher containerLauncher;

    private FilePath filePath;

    private boolean running;

    public CommandLineInterface() {

        this.fileHandler = new FileHandler();

        this.containerLauncher = new ContainerLauncher();
    }

    // ---------------------------------------------------------
    // WELCOME AND HELP
    // ---------------------------------------------------------

    public void displayWelcomeMessage(FilePath filePath) {

        String message = fileHandler.openResourceAndRead(
                filePath.welcome()
        );

        if (!message.isBlank()) {
            ShellLogger.log(message);
        }
    }

    public void displayHelpMessage(FilePath filePath) {

        String message = fileHandler.openResourceAndRead(
                filePath.help()
        );

        if (!message.isBlank()) {
            ShellLogger.log(message);
        }
    }

    // ---------------------------------------------------------
    // PLATFORM
    // ---------------------------------------------------------

    public void startPlatform() {

        ShellLogger.log("Launching PLATFORM JVM...");

        try {

            Process platformProcess =
                    containerLauncher.launchPlatform(
                            filePath.platformConf(),
                            List.of()
                    );

            Objects.requireNonNull(
                    platformProcess,
                    "Platform launcher returned null"
            );

            ShellLogger.log(
                    "PLATFORM JVM launched successfully."
            );

            ShellLogger.log(
                    "Platform PID: " + platformProcess.pid()
            );

        } catch (Exception exception) {

            ShellLogger.log(
                    "Unable to launch PLATFORM JVM: "
                            + exception.getMessage()
            );

            exception.printStackTrace();
        }
    }

    // ---------------------------------------------------------
    // WATCHDOG
    // ---------------------------------------------------------

    private void startWatchdog() {

        ShellLogger.log("Launching WATCHDOG JVM...");

        try {

            Process watchdogProcess =
                    containerLauncher.launchWatchdog(
                            filePath.watchdogConf(),
                            List.of()
                    );

            Objects.requireNonNull(
                    watchdogProcess,
                    "Watchdog launcher returned null"
            );

            ShellLogger.log(
                    "WATCHDOG JVM launched successfully."
            );

            ShellLogger.log(
                    "WATCHDOG PID: " + watchdogProcess.pid()
            );

        } catch (Exception exception) {

            ShellLogger.log(
                    "Unable to launch WATCHDOG JVM: "
                            + exception.getMessage()
            );

            exception.printStackTrace();
        }
    }

    // ---------------------------------------------------------
    // INSTALLATION VALIDATION
    // ---------------------------------------------------------

    public void validateInstallation() {

        try {

            // Internal JAR resources.
            validateResource(
                    filePath.welcome(),
                    "Welcome message"
            );

            validateResource(
                    filePath.help(),
                    "Help message"
            );

            // External configuration files.
            validateFile(
                    filePath.platformConf(),
                    "Platform configuration"
            );

            validateFile(
                    filePath.watchdogConf(),
                    "WATCHDOG configuration"
            );

            validateFile(
                    filePath.databaseConf(),
                    "Database configuration"
            );

            ShellLogger.log(
                    "Installation validation successful."
            );

        } catch (RuntimeException exception) {

            ShellLogger.log(
                    "Installation validation failed: "
                            + exception.getMessage()
            );
        }
    }

    private void validateResource(
            String resourceName,
            String description
    ) {

        Objects.requireNonNull(
                resourceName,
                description + " resource cannot be null"
        );

        String content =
                fileHandler.openResourceAndRead(resourceName);

        if (content.isBlank()) {

            throw new IllegalArgumentException(
                    description + " resource is empty: "
                            + resourceName
            );
        }
    }

    private void validateFile(
            String configuredPath,
            String description
    ) {

        Objects.requireNonNull(
                configuredPath,
                description + " path cannot be null"
        );

        Path path = ApplicationPaths.resolve(configuredPath);

        if (!Files.isRegularFile(path)
                || !Files.isReadable(path)) {

            throw new IllegalArgumentException(
                    description + " is not accessible: " + path
            );
        }
    }

    // ---------------------------------------------------------
    // DISPLAY INSTALLATION PATHS
    // ---------------------------------------------------------

    private void displayPaths() {

        ShellLogger.log("========================================");
        ShellLogger.log("          INSTALLATION PATHS            ");
        ShellLogger.log("========================================");

        ShellLogger.log(
                "Application home : "
                        + ApplicationPaths.getHome()
        );

        ShellLogger.log(
                "Welcome resource : " + filePath.welcome()
        );

        ShellLogger.log(
                "Help resource    : " + filePath.help()
        );

        ShellLogger.log(
                "Platform config  : "
                        + ApplicationPaths.resolve(
                        filePath.platformConf()
                )
        );

        ShellLogger.log(
                "WATCHDOG config  : "
                        + ApplicationPaths.resolve(
                        filePath.watchdogConf()
                )
        );

        ShellLogger.log(
                "Database config  : "
                        + ApplicationPaths.resolve(
                        filePath.databaseConf()
                )
        );

        ShellLogger.log("========================================");
    }

    // ---------------------------------------------------------
    // COMMAND DISPATCHER
    // ---------------------------------------------------------

    private void executeCommand(String command) {

        switch (command) {

            case "meng help" ->
                    displayHelpMessage(filePath);

            case "meng validate" ->
                    validateInstallation();

            case "meng paths" ->
                    displayPaths();

            case "meng start platform" ->
                    startPlatform();

            case "meng start watchdog" ->
                    startWatchdog();

            case "meng exit" ->
                    exitInstaller();

            case "" -> {
                // Ignore empty commands.
            }

            default ->
                    displayCommandNotFoundMessage(command);
        }
    }

    private void displayCommandNotFoundMessage(String command) {

        ShellLogger.log(
                "Unknown installer command: " + command
        );

        ShellLogger.log(
                "Use 'meng help' to see available commands."
        );
    }

    // ---------------------------------------------------------
    // EXIT
    // ---------------------------------------------------------

    private void exitInstaller() {

        ShellLogger.log(
                "Exiting installer. Launched JVMs will continue running."
        );

        running = false;
    }

    // ---------------------------------------------------------
    // LOAD INSTALLATION PATH CONFIGURATION
    // ---------------------------------------------------------

    private FilePath loadFilePaths(String[] args) {

        if (args == null || args.length > 1) {

            throw new IllegalArgumentException(
                    "Usage: mediation-engine [optional path override]"
            );
        }

        String configuredPath;

        if (args.length == 1) {

            configuredPath = args[0];

        } else {

            configuredPath = System.getProperty(
                    "mediation.paths",
                    DEFAULT_PATH_CONFIG
            );
        }

        Path path = ApplicationPaths.resolve(configuredPath);

        if (!Files.isRegularFile(path)
                || !Files.isReadable(path)) {

            throw new IllegalArgumentException(
                    "Installation path configuration is inaccessible: "
                            + path
            );
        }

        return Objects.requireNonNull(
                new FilePathParser().initializeFilePaths(
                        path.toString()
                ),
                "FilePathParser returned null"
        );
    }

    // ---------------------------------------------------------
    // SHELL LIFECYCLE
    // ---------------------------------------------------------

    public void launch() {

        launch(new String[0]);
    }

    public void launch(String[] args) {

        if (running) {

            throw new IllegalStateException(
                    "Installer is already running"
            );
        }

        filePath = loadFilePaths(args);

        running = true;

        displayWelcomeMessage(filePath);

        ShellLogger.log(
                "Type 'meng help' to see installer commands."
        );

        try (Scanner scanner = new Scanner(System.in)) {

            while (running && scanner.hasNextLine()) {

                String command = scanner.nextLine()
                        .trim()
                        .replaceAll("\\s+", " ");

                try {

                    executeCommand(command);

                } catch (RuntimeException exception) {

                    ShellLogger.log(
                            "Installer command failed: "
                                    + exception.getMessage()
                    );

                    exception.printStackTrace();
                }
            }
        }

        running = false;

        ShellLogger.log("Installer terminated.");
    }

    // ---------------------------------------------------------
    // MAIN
    // ---------------------------------------------------------

    public static void main(String[] args) {

        try {

            CommandLineInterface installer =
                    new CommandLineInterface();

            installer.launch(args);

        } catch (Exception exception) {

            ShellLogger.log(
                    "Installer initialization failed: "
                            + exception.getMessage()
            );

            exception.printStackTrace();
        }
    }
}
