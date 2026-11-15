package org.prats.handlers;
import org.junit.jupiter.api.*;
import org.prats.testing.TestDatabase;
import org.prats.parsers.MessageParser;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class DatabaseRequestHandlerTest {
    private final TestDatabase database=new TestDatabase();
    private final DatabaseRequestHandler handler=new DatabaseRequestHandler(database);
    private final MessageParser parser=new MessageParser(ProcessType.PLATFORM);
    @BeforeAll static void context(){TestDatabase.context();}
    @BeforeEach void table() throws Exception {try(var c=database.getDataSource().getConnection();var s=c.createStatement()){s.execute("CREATE TABLE items (id INT PRIMARY KEY, label VARCHAR(100))");}}
    private Message request(Operation operation,String json) throws Exception {
        Message request=parser.createEmptyMessage();request.setMessageType(MessageType.REQUEST);request.setOperation(operation);request.setPayload(new ObjectMapper().readTree(json));return request;
    }
    @Test void preparedCrudReturnsRowsAndPreservesCorrelation() throws Exception {
        Message create=request(Operation.CREATE,"{\"entity\":\"items\",\"data\":{\"id\":1,\"label\":\"O'Reilly\"}}");
        Message result=handler.handleRequest(create);assertEquals("SUCCESS",result.getStatus());assertEquals(create.getCorrelationId(),result.getCorrelationId());assertEquals(1,result.getPayload().path("affectedRows").asInt());
        Message fetch=handler.handleRequest(request(Operation.FETCH,"{\"entity\":\"items\"}"));
        assertEquals(1,fetch.getPayload().path("rowCount").asInt());assertEquals("O'Reilly",fetch.getPayload().path("rows").get(0).path("LABEL").asText());
        Message delete=handler.handleRequest(request(Operation.DELETE,"{\"entity\":\"items\",\"filters\":{\"id\":{\"operator\":\"EQ\",\"value\":1}}}"));assertEquals(1,delete.getPayload().path("affectedRows").asInt());
    }
    @Test void errorsAreTerminalAndDoNotExposeSqlOrCredentials() throws Exception {
        Message bad=handler.handleRequest(request(Operation.FETCH,"{\"entity\":\"missing\"}"));assertEquals("FAILED",bad.getStatus());assertTrue(bad.isResponseComplete());assertTrue(bad.getPayload().has("errorCode"));
        assertEquals("INVALID_REQUEST",handler.handleRequest(request(Operation.DELETE,"{\"entity\":\"items\"}")).getPayload().path("errorCode").asText());
        assertEquals("PROTECTED_ENTITY",handler.handleRequest(request(Operation.FETCH,"{\"entity\":\"users\"}")).getPayload().path("errorCode").asText());
        assertEquals("INVALID_REQUEST",handler.handleRequest(request(Operation.FETCH,"{\"entity\":\"items; DROP TABLE items\"}")).getPayload().path("errorCode").asText());
    }
    @Test void queryResultsAreBounded() throws Exception {
        try(var c=database.getDataSource().getConnection();var s=c.createStatement()){s.execute("INSERT INTO items SELECT X, 'row' FROM SYSTEM_RANGE(1,1100)");}
        var payload=handler.handleRequest(request(Operation.FETCH,"{\"entity\":\"items\"}")).getPayload();
        assertEquals(1000,payload.path("rowCount").asInt());assertTrue(payload.path("truncated").asBoolean());
    }
    @Test void accountStorageCapabilityCannotBeSuppliedInJson() throws Exception {
        Message request = request(Operation.FETCH, "{\"entity\":\"users\"}");
        request.setAccountDatabaseRequest(true);
        String serialized = parser.createJSONFromMessage(request);
        assertFalse(serialized.contains("accountDatabaseRequest"));
        Message decoded = parser.createMessageFromJSON(serialized.substring(0, serialized.length() - 1)
                + ",\"accountDatabaseRequest\":true}");
        assertFalse(decoded.isAccountDatabaseRequest());
        assertEquals("PROTECTED_ENTITY", handler.handleRequest(decoded).getPayload().path("errorCode").asText());
    }
}
