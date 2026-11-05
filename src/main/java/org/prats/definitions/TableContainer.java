package org.prats.definitions;

import java.util.List;
import java.util.Map;

// contains the table data
public class TableContainer {

    private final Table table;
    private final List<Map<String, String>> tableRecords;

    public TableContainer(
            Table table,
            List<Map<String, String>> tableRecords
    ) {
        this.table          = table;
        this.tableRecords   = tableRecords;
    }

    public Map<String, String> getRow(int row) {
        return tableRecords.get(row);
    }

    public int getRowCount() {
        return tableRecords.size();
    }

    public Table getTable() {
        return table;
    }
}
