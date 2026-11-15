package org.prats.testing;
import org.prats.database.databaseUtilities.DatabaseResourceManager;
import org.h2.jdbcx.JdbcDataSource;
import javax.sql.DataSource;
public final class TestDatabase implements DatabaseResourceManager {
    private final JdbcDataSource source = new JdbcDataSource();
    public TestDatabase() { source.setURL("jdbc:h2:mem:" + java.util.UUID.randomUUID() + ";DB_CLOSE_DELAY=-1"); }
    public void initialize() {}
    public void close() {}
    public DataSource getDataSource() { return source; }
    public static void context() {
        try { org.prats.definitions.classes.ProcessContext.getProcessName(); }
        catch(IllegalStateException e) { org.prats.definitions.classes.ProcessContext.initialize("platform"); }
    }
}
