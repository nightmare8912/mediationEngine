
package org.prats.jvmLauncher;

import org.prats.loggers.FileLogger;
import org.prats.utilities.ApplicationPaths;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class JVMLauncher {

    public Process launchJvm(
            String processName,
            List<String> jvmArgs,
            String classPath,
            String mainClass,
            List<String> applicationArgs
    ) throws IOException {

        Objects.requireNonNull(processName);
        Objects.requireNonNull(jvmArgs);
        Objects.requireNonNull(classPath);
        Objects.requireNonNull(mainClass);
        Objects.requireNonNull(applicationArgs);

        if (processName.isBlank() || mainClass.isBlank()) {
            throw new IllegalArgumentException(
                    "Process name and main class cannot be empty"
            );
        }

        Path applicationHome = ApplicationPaths.getHome();

        Path javaExecutable = Path.of(
                System.getProperty("java.home"),
                "bin",
                System.getProperty("os.name", "")
                        .toLowerCase(Locale.ROOT)
                        .contains("win")
                        ? "java.exe"
                        : "java"
        );

        Path resolvedClassPath =
                ApplicationPaths.resolve(classPath);

        if (!Files.isRegularFile(resolvedClassPath)
                || !Files.isReadable(resolvedClassPath)) {

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

        FileLogger.log(
                "Launching JVM: " + processName
        );

        FileLogger.log(
                "JVM command: " + command
        );

        ProcessBuilder processBuilder =
                new ProcessBuilder(command);

        processBuilder.directory(
                applicationHome.toFile()
        );

        processBuilder.inheritIO();

        return processBuilder.start();
    }
}
