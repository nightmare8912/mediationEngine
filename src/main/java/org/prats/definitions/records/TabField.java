
package org.prats.definitions.records;

public record TabField(
        String id,
        String label,
        String type,
        boolean required,
        boolean enabled
) {}
