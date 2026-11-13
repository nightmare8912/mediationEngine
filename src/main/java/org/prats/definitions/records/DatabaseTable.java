
package org.prats.definitions.records;

public record DatabaseTable(
        String tableLabel,
        String tableId,
        int creationSequence,
        boolean critical,
        String path
) {}
