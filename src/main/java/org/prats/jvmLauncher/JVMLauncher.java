package org.prats.jvmLauncher;

import org.prats.loggers.ShellLogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JVMLauncher {

    public Process launchJvm(
            String processName,
            List<String> jvmArgs,
            String classPath,
            String mainClass,
            List<String> applicationArgs
    ) throws IOException {

        List<String> command = new ArrayList<>();

        command.add("java");
        command.addAll(jvmArgs);

        command.add("-Dprocess.name="+processName);

        // command.add("-XX:ProcessLabel="+processName);

        command.add("-cp");
        command.add(classPath);

        command.add(mainClass);

        // application arguments
        command.addAll(applicationArgs);

        ShellLogger.log("Launch JVM command: " + command);

        ProcessBuilder processBuilder = new ProcessBuilder(command);

        // added for tests
        processBuilder.inheritIO();

        return processBuilder.start();
    }
}
