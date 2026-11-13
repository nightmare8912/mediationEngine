package org.prats.utilities;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class ApplicationPaths {

    private ApplicationPaths() {
    }

    public static Path getHome() {

        String configuredHome =
                System.getProperty("mediation.home");

        if (configuredHome != null && !configuredHome.isBlank()) {

            Path home = Path.of(configuredHome)
                    .toAbsolutePath()
                    .normalize();

            if (!Files.isDirectory(home)) {
                throw new IllegalStateException(
                        "Mediation Engine home is not a directory: " + home
                );
            }

            return home;
        }

        try {

            var codeSource =
                    ApplicationPaths.class
                            .getProtectionDomain()
                            .getCodeSource();

            if (codeSource == null) {
                throw new IllegalStateException(
                        "Cannot determine application location."
                );
            }

            Path location = Path.of(
                    codeSource.getLocation().toURI()
            ).toAbsolutePath().normalize();

            if (Files.isRegularFile(location)) {
                return location.getParent();
            }

            throw new IllegalStateException(
                    "Application must be launched from its packaged JAR "
                            + "or mediation.home must be configured."
            );

        } catch (URISyntaxException exception) {

            throw new IllegalStateException(
                    "Unable to resolve application directory.",
                    exception
            );
        }
    }

    public static Path resolve(String configuredPath) {

        if (configuredPath == null || configuredPath.isBlank()) {
            throw new IllegalArgumentException(
                    "Configured path cannot be empty."
            );
        }

        Path home = getHome();
        Path candidate = Path.of(configuredPath);

        Path resolved = candidate.isAbsolute()
                ? candidate.normalize()
                : home.resolve(candidate).normalize();

        resolved = resolved.toAbsolutePath().normalize();

        if (!resolved.startsWith(home)) {
            throw new IllegalArgumentException(
                    "Path escapes application directory: "
                            + configuredPath
            );
        }

        return resolved;
    }
}