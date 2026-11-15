package org.prats.jvmLauncher;

import org.prats.loggers.EngineLogger;
import org.prats.utilities.ApplicationPaths;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class JVMLauncher {

    private static final EngineLogger log =
            EngineLogger.getLogger(JVMLauncher.class);

    public Process launchJvm(
            String processName,
            List<String> jvmArgs,
            String classPath,
            String mainClass,
            List<String> applicationArgs
    ) throws IOException {

        log.debug(() ->
                "[JVMLauncher][launchJvm] JVM launch requested"
                        + " | processName=" + processName
                        + " | mainClass=" + mainClass
                        + " | jvmArgsCount=" + (jvmArgs == null ? 0 : jvmArgs.size())
                        + " | applicationArgsCount="
                        + (applicationArgs == null ? 0 : applicationArgs.size())
        );

        Objects.requireNonNull(processName);
        Objects.requireNonNull(jvmArgs);
        Objects.requireNonNull(classPath);
        Objects.requireNonNull(mainClass);
        Objects.requireNonNull(applicationArgs);

        if (processName.isBlank() || mainClass.isBlank()) {

            log.warn(() ->
                    "[JVMLauncher][launchJvm] JVM launch rejected because process name or main class is empty"
                            + " | processName=" + processName
                            + " | mainClass=" + mainClass
            );

            throw new IllegalArgumentException(
                    "Process name and main class cannot be empty"
            );
        }

        Path applicationHome = ApplicationPaths.getHome();

        log.debug(() ->
                "[JVMLauncher][launchJvm] Application home resolved"
                        + " | applicationHome=" + applicationHome
        );

        Path javaExecutable = Path.of(
                System.getProperty("java.home"),
                "bin",
                System.getProperty("os.name", "")
                        .toLowerCase(Locale.ROOT)
                        .contains("win")
                        ? "java.exe"
                        : "java"
        );

        log.debug(() ->
                "[JVMLauncher][launchJvm] Java executable resolved"
                        + " | javaExecutable=" + javaExecutable
                        + " | javaHome=" + System.getProperty("java.home")
        );

        Path resolvedClassPath =
                ApplicationPaths.resolve(classPath);

        log.debug(() ->
                "[JVMLauncher][launchJvm] Application classpath resolved"
                        + " | classPath=" + resolvedClassPath
        );

        if (!Files.isRegularFile(resolvedClassPath)
                || !Files.isReadable(resolvedClassPath)) {

            log.error(
                    "[JVMLauncher][launchJvm] Application JAR does not exist or is not readable"
                            + " | processName=" + processName
                            + " | resolvedClassPath=" + resolvedClassPath
            );

            throw new IllegalArgumentException(
                    "Application JAR does not exist or is not readable: "
                            + resolvedClassPath
            );
        }

        List<String> command = new ArrayList<>();

        command.add(javaExecutable.toString());

        command.addAll(jvmArgs);

        command.add(
                "-Dmediation.home=" + applicationHome
        );

        command.add(
                "-Dprocess.name=" + processName
        );

        command.add("-cp");

        command.add(resolvedClassPath.toString());

        command.add(mainClass);

        command.addAll(applicationArgs);

        log.info(() ->
                "[JVMLauncher][launchJvm] Launching JVM"
                        + " | processName=" + processName
                        + " | mainClass=" + mainClass
                        + " | applicationHome=" + applicationHome
        );

        log.debug(() ->
                "[JVMLauncher][launchJvm] JVM command constructed"
                        + " | command=" + command
        );

        ProcessBuilder processBuilder =
                new ProcessBuilder(command);

        processBuilder.directory(
                applicationHome.toFile()
        );

        processBuilder.inheritIO();

        log.debug(() ->
                "[JVMLauncher][launchJvm] ProcessBuilder configured"
                        + " | workingDirectory=" + applicationHome
                        + " | inheritIO=true"
        );

        Process process = processBuilder.start();

        log.info(() ->
                "[JVMLauncher][launchJvm] JVM process launched successfully"
                        + " | processName=" + processName
                        + " | pid=" + process.pid()
                        + " | alive=" + process.isAlive()
        );

        return process;
    }
}