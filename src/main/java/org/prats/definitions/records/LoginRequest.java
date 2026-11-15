package org.prats.definitions.records;

import java.util.Objects;

public record LoginRequest(
        String username,
        char[] password
) {

    public LoginRequest {

        Objects.requireNonNull(username, "Username cannot be null");
        Objects.requireNonNull(password, "Password cannot be null");

        if (username.isBlank()) {
            throw new IllegalArgumentException("Username cannot be blank");
        }

        password = password.clone();
    }

    @Override
    public char[] password() {
        return password.clone();
    }
}