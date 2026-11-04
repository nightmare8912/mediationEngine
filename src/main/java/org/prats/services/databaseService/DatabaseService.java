package org.prats.services.databaseService;

import com.zaxxer.hikari.HikariDataSource;
import org.prats.database.databaseUtilities.sqlHelper.SQLQuery;
import org.prats.database.h2Database.H2DatabaseInitializer;
import org.prats.definitions.Message;
import org.prats.definitions.MessageEntity;
import org.prats.definitions.ProcessType;
import org.prats.inputHandler.handlers.DatabaseMessageHandler;
import org.prats.services.Service;
import org.prats.services.messageService.MessageService;
import org.prats.threading.ThreadPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class DatabaseService implements Service {

    private HikariDataSource dataSource;
    private H2DatabaseInitializer databaseInitializer;
    private MessageService messageService;
    private final ProcessType process;
    private ExecutorService threadPool;
    private final DatabaseMessageHandler databaseMessageHandler;
    private final String serviceName;

    public DatabaseService(
            int coreThreads,
            int maxThreads,
            int idleTimeout,
            ProcessType process,
            String databaseConfigFile
    ) {
        this.serviceName            = this.getClass().getSimpleName();
        this.databaseInitializer    = new H2DatabaseInitializer(databaseConfigFile);
        this.databaseMessageHandler = new DatabaseMessageHandler(this);
        this.process                = process;
        threadPool                  = new ThreadPool().initializeAndGetThreadPool(
                coreThreads,
                maxThreads,
                idleTimeout
        );
    }

    @Override
    public Service startService() {
        databaseInitializer.initializeH2Database();
        dataSource = databaseInitializer.getH2DBConnectionPool();
        return this;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    // calling thread (message service) can submit the call to db thread and return to its pool
    public void submit(MessageEntity messageEntity) {
        threadPool.submit(() -> {
            databaseMessageHandler.handleMessage(messageEntity);
        });
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public void setMessageService(MessageService messageService) {
        this.messageService = messageService;
    }

    public void sendMessage(MessageEntity messageEntity) {
        messageService.submit(
            messageEntity
        );
    }

    @Override
    public void shutdownService() {

        threadPool.shutdown();

        try {
            if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        if (dataSource != null) {
            dataSource.close();
        }
    }
}
