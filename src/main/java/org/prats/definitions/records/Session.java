package org.prats.definitions.records;

import org.prats.definitions.enums.Permission;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public record Session(
        UUID sessionId,
        String username,
        Set<Permission> permissions,
        Instant createdAt,
        Instant expiresAt
) {
    public Session {
        Objects.requireNonNull(sessionId);
        Objects.requireNonNull(username);
        permissions = Set.copyOf(permissions);
        Objects.requireNonNull(createdAt);
        Objects.requireNonNull(expiresAt);
    }

    public boolean hasPermission(Permission permission) {
        return permissions.contains(permission);
    }

    public boolean isExpired() {
        return !Instant.now().isBefore(expiresAt);
    }
}
