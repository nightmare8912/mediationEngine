package org.prats.database.databaseUtilities;

import javax.sql.DataSource;

public interface DatabaseResourceManager {
    void initialize();
    void close();
    DataSource getDataSource();
}
