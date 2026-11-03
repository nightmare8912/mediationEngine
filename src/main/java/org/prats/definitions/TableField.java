
package org.prats.definitions;

public record TableField(
        String id,
        String label,
        String type,
        boolean required,
        boolean primaryKey
) {}
