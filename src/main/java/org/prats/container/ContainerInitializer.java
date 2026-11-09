package org.prats.container;


import org.prats.database.databaseUtilities.HikariDBConnectionPool.HikariDBConnectionPool;
import org.prats.database.h2Database.H2DatabaseResourceManager;
import org.prats.definitions.ContainerType;
import org.prats.definitions.ProcessType;
import org.prats.forwarders.DatabaseForwarder;
import org.prats.handlers.DatabaseRequestHandler;
import org.prats.parsers.MessageParser;
import org.prats.parsers.CommandParser;
import org.prats.processors.CommandProcessor;
import org.prats.processors.MessageProcessor;
import org.prats.processors.RequestProcessor;
import org.prats.processors.ResponseProcessor;
import org.prats.registries.ContainerServiceRegistry;
import org.prats.registries.CorrelationRegistry;
import org.prats.registries.RetryRegistry;
import org.prats.registries.ServiceRegistry;
import org.prats.services.HealthReportAnalyzer;
import org.prats.services.containerService.ContainerService;
import org.prats.services.databaseService.DatabaseService;
import org.prats.services.healthCheckService.HealthService;
import org.prats.services.messageService.MessageService;
import org.prats.services.tcpService.TCPService;

import java.util.List;
import java.util.Set;

public class ContainerInitializer {

    private ContainerService containerService;
    private int tcpPort, tcpBacklog, coreThreads, idleTimeout, maxThreads;
    private String databaseConfigFile;
    private ContainerType containerType;
    private String containerName;
    private ServiceRegistry serviceRegistry;

    private void setParametersFromArgs(String[] args) {
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
            if (arg.startsWith("--container-name=")) {
                containerName = arg.substring("--container-name=".length());
            }
            if (arg.startsWith("--container-type=")) {
                containerType = ContainerType.valueOf(arg.substring("--container-type=".length()));
            }
        }
    }

    private void initializeServiceRegistryAndDependencies() {

        ProcessType processType                             = containerType == ContainerType.PLATFORM ? ProcessType.PLATFORM : ProcessType.EC;

        CorrelationRegistry correlationRegistry             = new CorrelationRegistry();
        MessageParser   messageParser                       = new MessageParser(processType);
        CommandParser   commandParser                       = new CommandParser(processType);

        RetryRegistry retryRegistry                         = new RetryRegistry(5);

        ResponseProcessor responseProcessor                 = new ResponseProcessor(correlationRegistry, retryRegistry, messageParser);
        CommandProcessor commandProcessor                   = new CommandProcessor(commandParser, responseProcessor, Set.of("status", "restart services"));

        HikariDBConnectionPool hikariDBConnectionPool       = new HikariDBConnectionPool();
        H2DatabaseResourceManager h2DatabaseResourceManager = new H2DatabaseResourceManager(databaseConfigFile, hikariDBConnectionPool);

        DatabaseRequestHandler databaseRequestHandler       = new DatabaseRequestHandler(h2DatabaseResourceManager);

        DatabaseService databaseService                     = new DatabaseService(coreThreads, maxThreads, idleTimeout, h2DatabaseResourceManager, databaseRequestHandler);

        DatabaseForwarder databaseForwarder                 = new DatabaseForwarder(databaseService);
        MessageProcessor messageProcessor                   = new MessageProcessor(databaseForwarder);

        RequestProcessor requestProcessor                   = new RequestProcessor(correlationRegistry, messageParser, commandProcessor, messageProcessor);

        MessageService messageService                       = new MessageService(coreThreads, maxThreads, idleTimeout, 5, 15, 200, 120, requestProcessor, responseProcessor);
        TCPService tcpService                               = new TCPService(coreThreads, maxThreads, idleTimeout, tcpPort, tcpBacklog, containerName, List.of(), messageService, messageParser);

        serviceRegistry                                     = new ContainerServiceRegistry();
        HealthService   healthService                       = new HealthService(serviceRegistry, 100, 75);

        HealthReportAnalyzer healthReportAnalyzer           = new HealthReportAnalyzer();


        commandProcessor.setRequestProcessor(requestProcessor);
        databaseService.setMessageDispatcher(messageService);

        containerService = new ContainerService(
                coreThreads,
                maxThreads,
                idleTimeout,
                tcpPort,
                tcpBacklog,
                containerName,
                databaseConfigFile,
                containerType,
                serviceRegistry,
                healthService,
                healthReportAnalyzer,
                List.of(),
                this
        );

        healthService.setServiceFailureReporter(containerService);

        serviceRegistry.registerService(databaseService);
        serviceRegistry.registerService(messageService);
        serviceRegistry.registerService(tcpService);
        serviceRegistry.registerService(healthService);
    }

    // this initializes the container service which is the parent service.
    // even though it is parent it doesnt know which services do what and they are responsible for what
    // it's primary responsibility is to manage the lif-cycle of all the service that are in its service registry
    // in some cases where it sees all its services are failing, it can decide to kill itself
    // which will include clearing its registries and initializing again
    // if this doesnt work, and if the container service is not a platform jvm,
    // the platform jvm which is parent of all container jvms can kill the affected jvm and re-create it
    // i.e, restart the entire jvm along with all its services
    private void initializeContainer(String[] args) {

        setParametersFromArgs(args);

        initializeServiceRegistryAndDependencies();

        // the container service will start all the services inside its service registry
        containerService.start();
    }

    // fn to initialize a container service
    public static void main(String[] args) {
        new ContainerInitializer().initializeContainer(
                args
        );
    }
}
