
package org.prats.definitions;

public record DatabaseTable(
        String tableLabel,
        String tableId,
        int creationSequence,
        boolean critical,
        String path
) {}
