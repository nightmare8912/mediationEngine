package org.prats.database.databaseUtilities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.prats.definitions.MessageDefinition;
import org.prats.definitions.OperationDefinition;
import org.prats.messageHandler.MessageParser;
import org.prats.definitions.ProcessTypeDefinition;
import org.prats.definitions.MessageTypeDefinition;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DatabaseUtils {
    private MessageParser messageParser;
    private ProcessTypeDefinition processTypeDefinition;
    private ObjectMapper objectMapper;

    public DatabaseUtils(ProcessTypeDefinition processTypeDefinition) {
        this.objectMapper   = new ObjectMapper();
        this.processTypeDefinition = processTypeDefinition;
        messageParser       = new MessageParser(processTypeDefinition);
    }

    public String createMessageFromDBResponse(MessageDefinition originalMessage, ResultSet rs)
            throws SQLException, JsonProcessingException {

        List<Map<String, Object>> rows = new ArrayList<>();
        ResultSetMetaData metaData     = rs.getMetaData();
        int columnCount                = metaData.getColumnCount();
        Map<String, Object> row = new LinkedHashMap<>();


        for (int i = 0; i < columnCount; ++i) {
            row.put(
              metaData.getColumnLabel(i),
              rs.getObject(i)
            );
            rows.add(row);
        }
        return
                messageParser.createJSONFromMessage(
                        new MessageDefinition(
                                MessageTypeDefinition.INFO,
                                processTypeDefinition,
                                originalMessage.getOperation(),
                                objectMapper.valueToTree(rows),
                                originalMessage.getCorrelationId()
                        )
                );

    }
}