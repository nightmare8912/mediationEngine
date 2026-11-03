
package org.prats.definitions;

import java.util.List;

public record Database(
        String label,
        String id,
        String url,
        String user,
        String pass,
        long timeout,
        long idleTimeout,
        int maxConnections,
        List<DatabaseTable> tables
) {
    public Database {
        tables = List.copyOf(tables);
    }
}
