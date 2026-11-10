package org.prats.handlers;

import com.zaxxer.hikari.HikariDataSource;
import org.prats.database.databaseUtilities.DatabaseResourceManager;
import org.prats.database.databaseUtilities.MessageSQLParser;
import org.prats.database.databaseUtilities.sqlHelper.SQLQuery;
import org.prats.definitions.Message;
import org.prats.definitions.Operation;
import org.prats.loggers.ShellLogger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseRequestHandler implements RequestHandler {

    private final DatabaseResourceManager databaseResourceManager;
    private final MessageSQLParser messageSQLParser;

    public DatabaseRequestHandler(
            DatabaseResourceManager databaseResourceManager
    ) {
        this.databaseResourceManager    = databaseResourceManager;
        this.messageSQLParser           = new MessageSQLParser();
    }

    public ResultSet executeQuery(SQLQuery sqlQuery, Connection connection) {
        return null;
    }

    public int executeUpdate(SQLQuery sqlQuery, Connection connection) {
        return 0;
    }

    public Message parseDatabaseQueryResponseToMessage(Message requestMessage, ResultSet resultSet) {
        return null;
    }

    public Message parseDatabaseUpdateResponseToMessageEntity(Message requestMessage, int result) {
        return null;
    }

    private Message generateResponseError() {
        return null;
    }

    private Message handleQuery(Message message, SQLQuery sqlQuery) {
        DataSource dataSource = databaseResourceManager.getDataSource();
        try (Connection connection = dataSource.getConnection()) {
            return parseDatabaseQueryResponseToMessage(
                    message,
                    executeQuery(sqlQuery, connection)
            );
        } catch (SQLException sqlException) {
            ShellLogger.log(
                    "[DatabaseRequestHandler] [handleQuery], Exception: "
                            + sqlException.getMessage()
            );
        }
        return null; // TODO: return proper error response
    }

    private Message handleUpdate(Message message, SQLQuery sqlQuery) {
        DataSource dataSource = databaseResourceManager.getDataSource();
        try (Connection connection = dataSource.getConnection()) {

            return parseDatabaseUpdateResponseToMessageEntity(
                    message,
                    executeUpdate(sqlQuery, connection)
            );
        } catch (SQLException sqlException) {

            ShellLogger.log(
                    "[DatabaseRequestHandler] [handleUpdate], Exception: "
                            + sqlException.getMessage()
            );
        }
        return null; // TODO: return proper error response
    }

    @Override
    public Message handleRequest(Message message) {

        SQLQuery sqlQuery   = messageSQLParser.createSQLFromMessage(message);

        return switch (message.getOperation()) {
            case FETCH, SEARCH          -> handleQuery(message, sqlQuery);
            case CREATE, UPDATE, DELETE -> handleUpdate(message, sqlQuery);
            default -> null; // todo: return proper error here
        };
    }
}
