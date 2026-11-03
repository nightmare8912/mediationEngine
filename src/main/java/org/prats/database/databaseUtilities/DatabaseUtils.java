package org.prats.database.databaseUtilities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.prats.definitions.Message;
import org.prats.inputHandler.utils.MessageParser;
import org.prats.definitions.ProcessType;
import org.prats.definitions.MessageType;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DatabaseUtils {
    private MessageParser messageParser;
    private ProcessType processType;
    private ObjectMapper objectMapper;

    public DatabaseUtils(ProcessType processType) {
        this.objectMapper   = new ObjectMapper();
        this.processType = processType;
        messageParser       = new MessageParser(processType);
    }

    public String createMessageFromDBResponse(Message originalMessage, ResultSet rs)
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
                        new Message(
                                MessageType.INFO,
                                processType,
                                originalMessage.getOperation(),
                                objectMapper.valueToTree(rows),
                                originalMessage.getCorrelationId()
                        )
                );

    }
}