
package org.prats.definitions;

import java.util.List;

public record Tab(
        String label,
        List<TabField> fields
) {
    public Tab {
        fields = List.copyOf(fields);
    }
}
