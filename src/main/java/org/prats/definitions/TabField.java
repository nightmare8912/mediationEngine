
package org.prats.definitions;

public record TabField(
        String id,
        String label,
        String type,
        boolean required,
        boolean enabled
) {}
