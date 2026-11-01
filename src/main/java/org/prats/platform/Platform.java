package org.prats.platform;

import org.prats.threading.ThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class Platform {

    private static int tcpPort, tcpBacklog, coreThreads, idleTimeout, maxThreads, coreIdleTimeout;

    private List<ContainerProcess> containerProcess;
    private static ExecutorService threadPool;

    public Platform() { this.containerProcess = new ArrayList<>(); }

    private void initializePlatformParamsFromArgs(String[] args) {
        for (String arg: args) {
            if (arg.startsWith("--tcp-port=")) {
                tcpPort         = Integer.parseInt(arg.substring("--tcp-port=".length()));
            }
            if (arg.startsWith("--tcp-backlog=")) {
                tcpBacklog      = Integer.parseInt(arg.substring("--tcp-backlog=".length()));;
            }
            if (arg.startsWith("--core-threads=")) {
                coreThreads     = Integer.parseInt(arg.substring("--core-threads=".length()));
            }
            if (arg.startsWith("--max-threads=")) {
                maxThreads      = Integer.parseInt(arg.substring("--max-threads=".length()));
            }
            if (arg.startsWith("--idle-timeout=")) {
                idleTimeout     = Integer.parseInt(arg.substring("--idle-timeout=".length()));
            }
            if (arg.startsWith("--core-idle-timeout=")) {
                coreIdleTimeout = Integer.parseInt(arg.substring("--core-idle-timeout=".length()));
            }
        }
    }

    public void validatePlatformArgs() throws IllegalArgumentException {
        // validating the values
        if (tcpPort < 1 || tcpPort > 65535) {
            throw new IllegalArgumentException("Invalid value received by platform for TCP port: " + tcpPort);
        }
        if (tcpBacklog < 1) {
            throw new IllegalArgumentException("Invalid value received by platform for TCP backlog: " + tcpBacklog);
        }
        if (coreThreads < 1) {
            throw new IllegalArgumentException("Invalid value received by platform for Core Threads: " + coreThreads);
        }
        if (idleTimeout < 1) {
            throw new IllegalArgumentException("Invalid value received by platform for Idle Timeout: " + idleTimeout);
        }
        if (maxThreads < 1) {
            throw new IllegalArgumentException("Invalid value received by platform for Max Threads: " + maxThreads);
        }
    }

    public void startServices() {

    }

    public static void main(String[] args) throws IllegalArgumentException, IOException {
        // initializing the application params
        int tcpPort     = -1;
        int tcpBacklog  = -1;
        Platform platform = new Platform();

        // reading application input args to set up the platform application [the platform jvm has already been set up]
        platform.initializePlatformParamsFromArgs(args);

        threadPool = new ThreadPool().initializeAndGetThreadPool(
                    coreThreads,
                    maxThreads,
                    idleTimeout,
                    coreIdleTimeout
        );

        try(
                ServerSocket serverSocket = new ServerSocket(
                tcpPort,
                tcpBacklog,
                InetAddress.getLoopbackAddress()
        )) {

            while (true) {

            }
        }

    }
}
