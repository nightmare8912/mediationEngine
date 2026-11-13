package org.prats.handlers;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileHandler {

    private String path;

    public FileHandler() {
    }

    public FileHandler(String path) {
        this.path = path;
    }

    // Reads an internal resource bundled inside the JAR.
    public String openResourceAndRead(String resourceName) {

        String normalizedName = resourceName.startsWith("/")
                ? resourceName.substring(1)
                : resourceName;

        try (InputStream input =
                     FileHandler.class.getClassLoader()
                             .getResourceAsStream(normalizedName)) {

            if (input == null) {
                throw new IllegalArgumentException(
                        "Internal resource not found: " + resourceName
                );
            }

            return new String(
                    input.readAllBytes(),
                    StandardCharsets.UTF_8
            );

        } catch (IOException exception) {

            throw new UncheckedIOException(
                    "Unable to read internal resource: " + resourceName,
                    exception
            );
        }
    }

    // Existing filesystem-reading functionality.
    public String openAndRead() {

        if (path == null || path.isBlank()) {
            throw new IllegalStateException(
                    "File path has not been configured."
            );
        }

        try {
            return Files.readString(Path.of(path));

        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    public String openAndRead(String path) {
        this.path = path;
        return openAndRead();
    }
}