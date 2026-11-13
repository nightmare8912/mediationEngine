
package org.prats.installer;

import org.prats.definitions.records.FilePath;
import org.prats.container.launchers.ContainerLauncher;
import org.prats.handlers.FileHandler;
import org.prats.loggers.ShellLogger;
import org.prats.parsers.configParser.FilePathParser;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;

public class CommandLineInterface {

    private static final String DEFAULT_PATH_CONFIG =
            "src/main/resources/installer/allFilePath.conf";

    private final FileHandler fileHandler;
    private final ContainerLauncher containerLauncher;

    private FilePath filePath;

    private boolean running;


    public CommandLineInterface() {

        this.fileHandler = new FileHandler();

        this.containerLauncher = new ContainerLauncher();
    }


    public void displayWelcomeMessage(FilePath filePath) {

        String message = fileHandler.openAndRead(
                filePath.welcome()
        );

        if (message != null && !message.isBlank()) {
            ShellLogger.log(message);
        }
    }


    public void displayHelpMessage(FilePath filePath) {

        String message = fileHandler.openAndRead(
                filePath.help()
        );

        if (message != null && !message.isBlank()) {
            ShellLogger.log(message);
        }
    }

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

    private void startWatchdog() {
        ShellLogger.log("Launching Watchdog JVM...");

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
                    "Watchdog JVM launched successfully."
            );

            ShellLogger.log(
                    "Watchdog PID: " + watchdogProcess.pid()
            );

        } catch (Exception exception) {

            ShellLogger.log(
                    "Unable to launch Watchdog JVM: "
                            + exception.getMessage()
            );

            exception.printStackTrace();
        }
    }


    public void validateInstallation() {

        try {

            validateFile(
                    filePath.welcome(),
                    "Welcome message"
            );

            validateFile(
                    filePath.help(),
                    "Help message"
            );

            validateFile(
                    filePath.platformConf(),
                    "Platform configuration"
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


    private void validateFile(
            String configuredPath,
            String description
    ) {

        Objects.requireNonNull(
                configuredPath,
                description + " path cannot be null"
        );

        Path path = Path.of(configuredPath)
                .toAbsolutePath()
                .normalize();

        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {

            throw new IllegalArgumentException(
                    description + " is not accessible: " + path
            );
        }
    }


    private void displayPaths() {

        ShellLogger.log("========================================");
        ShellLogger.log("          INSTALLATION PATHS            ");
        ShellLogger.log("========================================");

        ShellLogger.log(
                "Welcome message : " + filePath.welcome()
        );

        ShellLogger.log(
                "Help message    : " + filePath.help()
        );

        ShellLogger.log(
                "Platform config : " + filePath.platformConf()
        );

        ShellLogger.log("========================================");
    }


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

            case "meng exit" ->
                    exitInstaller();

            case "meng start watchdog" ->
                startWatchdog();

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


    private void exitInstaller() {

        ShellLogger.log(
                "Exiting installer. Launched JVMs will continue running."
        );

        running = false;
    }

    private FilePath loadFilePaths(String[] args) {

        if (args == null || args.length > 1) {

            throw new IllegalArgumentException(
                    "Usage: installer.jar [path-to-allFilePath.conf]"
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

        Path path = Path.of(configuredPath)
                .toAbsolutePath()
                .normalize();

        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {

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

        Scanner scanner = new Scanner(System.in);

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

        running = false;

        ShellLogger.log("Installer terminated.");
    }

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
