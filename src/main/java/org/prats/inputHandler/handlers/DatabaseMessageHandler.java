package org.prats.inputHandler.handlers;

import org.prats.database.databaseUtilities.MessageSQLParser;
import org.prats.database.databaseUtilities.sqlHelper.SQLQuery;
import org.prats.definitions.MessageEntity;
import org.prats.definitions.Operation;
import org.prats.services.databaseService.DatabaseService;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseMessageHandler implements MessageHandler {

    private final MessageSQLParser messageSQLParser;
    private final DatabaseService databaseService;

    public DatabaseMessageHandler(
            DatabaseService databaseService
    ) {
        this.messageSQLParser = new MessageSQLParser();
        this.databaseService = databaseService;
    }

    public ResultSet executeQuery(SQLQuery sqlQuery, Connection connection) {
        return null;
    }

    public int executeUpdate(SQLQuery sqlQuery, Connection connection) {
        return 0;
    }

    public MessageEntity parseDatabaseQueryResponseToMessageEntity(MessageEntity requestMessage, ResultSet resultSet) {
        return null;
    }

    public MessageEntity parseDatabaseUpdateResponseToMessageEntity(MessageEntity requestMessage, int result) {
        return null;
    }

    @Override
    public void handleMessage(MessageEntity messageEntity) {

        SQLQuery sqlQuery   = messageSQLParser.createSQLFromMessage(messageEntity);
        Operation operation = messageEntity.getMessage().getOperation();
        try (Connection connection = databaseService.getConnection()) {
            switch (operation) {
                case FETCH, SEARCH ->   sendResponse(parseDatabaseQueryResponseToMessageEntity(messageEntity,    executeQuery(sqlQuery, connection)));
                default ->              sendResponse(parseDatabaseUpdateResponseToMessageEntity(messageEntity,   executeUpdate(sqlQuery, connection)));
            }
        } catch (SQLException sqlException) {

        }
    }

    private void sendResponse(MessageEntity messageEntity) {
        databaseService.sendMessage(messageEntity);
    }
}
