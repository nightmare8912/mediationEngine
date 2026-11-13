
package org.prats.definitions.records;

import java.util.List;

public record Table(
        String label,
        String id,
        List<TableField> fields
) {
    public Table {
        fields = List.copyOf(fields);
    }

    public int numberOfFields() {
        return fields.size();
    }
}
