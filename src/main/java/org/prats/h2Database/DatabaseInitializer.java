package org.prats.h2Database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseInitializer {
    public static Connection getConnection() throws SQLException {
        String url = "jdbc:h2:file:./data/fileprocessor";
        return DriverManager.getConnection(url, "sa", "");
    }
}
