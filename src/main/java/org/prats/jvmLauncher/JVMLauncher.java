package org.prats.jvmLauncher;

import org.prats.definitions.PlatformDefinition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JVMLauncher {

    public Process launchJvm(List<String> jvmArgs, String classPath, List<String> applicationArgs) throws IOException {

        List<String> command = new ArrayList<>();
        command.addAll(jvmArgs);
        command.add("-cp");
        command.add(classPath);

        command.addAll(applicationArgs);

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        return processBuilder.start();
    }
}
