package org.prats.database.databaseUtilities.sqlHelper;

import java.util.List;

public record SQLQuery(
        String query,
        List<Object> parameters
) {
    public SQLQuery {
        parameters = List.copyOf(parameters);
    }
}
