
package org.prats.definitions.records;

public record TableField(
        String id,
        String label,
        String type,
        boolean required,
        boolean primaryKey
) {}
