import org.prats.configParser.DatabaseConfigParser;
import org.prats.definitions.DatabaseDefinition;
import org.prats.database.h2Database.DatabaseInitializer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Test {
    public static void main(String[] args) throws SQLException {
//        TableInitializer tableInitializer = new TableInitializer("/Users/pratyush./Desktop/coding/mediationEngine2/src/main/resources/configs/databaseConfigs/table_database.conf");
//        tableInitializer.initializeDatabase();

        DatabaseDefinition databaseDefinition = new DatabaseConfigParser().initializeDatabaseConfig("/Users/pratyush./Desktop/coding/mediationEngine2/src/main/resources/configs/databaseConfigs/table_database.conf");
        try (Connection connection = DatabaseInitializer.getConnection(
                databaseDefinition.getUrl(),
                databaseDefinition.getUser(),
                databaseDefinition.getPass()
        )) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "SELECT * FROM batch_agents"
            );

            while (resultSet.next()) {
                System.out.println(
                        resultSet.getString("agent_id")
                );
            }
        } catch (SQLException sqle) {
            System.out.println(sqle.getStackTrace());
        }
    }
}
