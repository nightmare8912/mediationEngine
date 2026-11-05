package org.prats.platform;

import org.prats.loggers.ShellLogger;
import org.prats.definitions.ProcessType;
import org.prats.services.PlatformServiceRegistry;
import org.prats.services.ServiceRegistry;
import org.prats.services.databaseService.DatabaseService;
import org.prats.services.healthCheckService.HealthService;
import org.prats.services.messageService.MessageService;
import org.prats.services.tcpService.TCPService;
import org.prats.threading.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class Platform {

    private static int tcpPort, tcpBacklog, coreThreads, idleTimeout, maxThreads;
    private static String databaseConfigFile;

    private static List<ContainerProcess> containerProcess;
    private static ExecutorService threadPool;
    private static DatabaseService databaseService;
    private static MessageService messageService;
    private static TCPService tcpService;
    private static HealthService healthService;
    private static ServiceRegistry serviceRegistry;

    public Platform() { this.containerProcess = new ArrayList<>(); }

    // initializes all the platform class arguments declared above, well not all, but some
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
            if (arg.startsWith("--database-conf=")) {
                databaseConfigFile = arg.substring("--database-conf=".length());
            }
//            if (arg.startsWith("--core-idle-timeout=")) {
//                coreIdleTimeout = Integer.parseInt(arg.substring("--core-idle-timeout=".length()));
//            }
        }
    }

    public static void validatePlatformArgs() throws IllegalArgumentException {
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

    public static void initializeDummyPlatformParams() {
        tcpPort             = 15000;
        tcpBacklog          = 50;
        coreThreads         = 10;
        maxThreads          = 15;
        idleTimeout         = 60;
        databaseConfigFile  = "/Users/pratyush./Desktop/coding/mediationEngine2/src/main/resources/configs/databaseConfigs/table_database.conf";
    }

    // this is messy to wrap my head around, but messy is good?
    public static void startServices() {

        serviceRegistry = new PlatformServiceRegistry();

        databaseService = new DatabaseService(
                coreThreads,
                maxThreads,
                idleTimeout,
                databaseConfigFile
        );

        // the tcp service would hand over message to message service for it to take care
        tcpService = new TCPService(
                tcpPort,
                tcpBacklog,
                coreThreads,
                maxThreads,
                idleTimeout,
                ProcessType.PLATFORM
        );

        // the message service needs to hand back the message to tcp service once it is ready to respond, for the tcp service to write it back to the socket
        messageService = new MessageService(
                coreThreads,
                maxThreads,
                idleTimeout,
                ProcessType.PLATFORM,
                tcpService
        );

        healthService = new HealthService(
                serviceRegistry,
                50,
                40
        );

        // once initializing the message service, we need to pass a reference of it to the tcp service so that it can pass the message to it
        tcpService.setMessageService(messageService);
        databaseService.setMessageService(messageService);
        messageService.setServiceRegistry(serviceRegistry);


        serviceRegistry.registerService(databaseService);
        serviceRegistry.registerService(messageService);
        serviceRegistry.registerService(tcpService);
        serviceRegistry.registerService(healthService);

        // starting all the services
        ShellLogger.log("Starting database service");
        databaseService.startService();
        ShellLogger.log("Starting message service");
        messageService.startService();
        ShellLogger.log("Starting tcp service");
        tcpService.startService();
        ShellLogger.log("Starting health check service");
        healthService.startService();
    }

    // shuts down all the services
    public static void shutdownServices() {
//         commented out for test
//        ShellLogger.log("shutting down health check service");
//        healthCheckService.shutdownService();

        ShellLogger.log("shutting down tcp service");
        tcpService.shutdownService();

        ShellLogger.log("shutting down database service");
        databaseService.shutdownService();

        ShellLogger.log("shutting down message service");
        messageService.shutdownService();

        serviceRegistry.clearRegistry();
    }

    private static void initiateShutdown() {
        shutdownServices();
        ShellLogger.log("Killing the platform...");
        System.exit(0);
    }

    public static void  shutdown() {
        threadPool.submit(Platform::initiateShutdown);
    }

    private static void initiateRestartServices() {
        shutdownServices();
        ShellLogger.log("Attempting to restart services...");
        startServices();
    }

    public static void restartServices() {
        threadPool.submit(Platform::initiateRestartServices);
    }

    public static void main(String[] args) throws IllegalArgumentException, IOException {
        // initializing the application params
        int tcpPort       = -1;
        int tcpBacklog    = -1;
        Platform platform = new Platform();

        // reading application input args to set up the platform application [the platform jvm has already been set up
         platform.initializePlatformParamsFromArgs(args);

         // for testing
         try {
             validatePlatformArgs();
         } catch (IllegalArgumentException ilexe) {
             initializeDummyPlatformParams();
         }
         // please remove -- for testing


        validatePlatformArgs();
        threadPool = new ThreadPool().initializeAndGetThreadPool(
                    coreThreads,
                    maxThreads,
                    idleTimeout,
                    true
        );

        startServices();
    }
}
