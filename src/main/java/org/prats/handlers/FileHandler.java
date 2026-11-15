package org.prats.handlers;

import org.prats.loggers.EngineLogger;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileHandler {

    private static final EngineLogger log =
            EngineLogger.getLogger(FileHandler.class);

    private String path;

    public FileHandler() {
        log.debug(
                "[FileHandler][constructor] File handler initialized without a configured path"
        );
    }

    public FileHandler(String path) {
        this.path = path;

        log.debug(() ->
                "[FileHandler][constructor] File handler initialized"
                        + " | path=" + path
        );
    }

    // Reads an internal resource bundled inside the JAR.
    public String openResourceAndRead(String resourceName) {

        log.debug(() ->
                "[FileHandler][openResourceAndRead] Reading internal JAR resource"
                        + " | resourceName=" + resourceName
        );

        String normalizedName = resourceName.startsWith("/")
                ? resourceName.substring(1)
                : resourceName;

        try (InputStream input =
                     FileHandler.class.getClassLoader()
                             .getResourceAsStream(normalizedName)) {

            if (input == null) {

                log.warn(() ->
                        "[FileHandler][openResourceAndRead] Internal resource not found"
                                + " | resourceName=" + resourceName
                                + " | normalizedName=" + normalizedName
                );

                throw new IllegalArgumentException(
                        "Internal resource not found: " + resourceName
                );
            }

            String content = new String(
                    input.readAllBytes(),
                    StandardCharsets.UTF_8
            );

            log.info(() ->
                    "[FileHandler][openResourceAndRead] Internal resource read successfully"
                            + " | resourceName=" + normalizedName
                            + " | charactersRead=" + content.length()
            );

            return content;

        } catch (IOException exception) {

            log.error(
                    "[FileHandler][openResourceAndRead] Unable to read internal resource"
                            + " | resourceName=" + resourceName
                            + " | normalizedName=" + normalizedName,
                    exception
            );

            throw new UncheckedIOException(
                    "Unable to read internal resource: " + resourceName,
                    exception
            );
        }
    }

    // Existing filesystem-reading functionality.
    public String openAndRead() {

        log.debug(() ->
                "[FileHandler][openAndRead] Reading filesystem resource"
                        + " | path=" + path
        );

        if (path == null || path.isBlank()) {

            log.warn(
                    "[FileHandler][openAndRead] File read rejected because path has not been configured"
            );

            throw new IllegalStateException(
                    "File path has not been configured."
            );
        }

        try {
            String content = Files.readString(Path.of(path));

            log.info(() ->
                    "[FileHandler][openAndRead] Filesystem resource read successfully"
                            + " | path=" + path
                            + " | charactersRead=" + content.length()
            );

            return content;

        } catch (IOException exception) {

            log.error(
                    "[FileHandler][openAndRead] Unable to read filesystem resource"
                            + " | path=" + path,
                    exception
            );

            throw new UncheckedIOException(exception);
        }
    }

    public String openAndRead(String path) {

        this.path = path;

        log.debug(() ->
                "[FileHandler][openAndRead] Filesystem path configured for reading"
                        + " | path=" + path
        );

        return openAndRead();
    }
}