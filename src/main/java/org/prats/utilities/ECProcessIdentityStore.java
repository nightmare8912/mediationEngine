
package org.prats.utilities;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;

/**
 * Atomic, verifiable EC identity records.
 * Stale files are never proof of liveness.
 */
public final class ECProcessIdentityStore {

    private final Path home;

    public ECProcessIdentityStore(Path home) {
        this.home = home.toAbsolutePath().normalize();
    }

    public record Identity(
            long pid,
            Instant startedAt,
            String containerId,
            String applicationHome
    ) {

        public boolean matches(ProcessHandle process) {
            return process.isAlive()
                    && process.pid() == pid
                    && process.info()
                    .startInstant()
                    .filter(startedAt::equals)
                    .isPresent();
        }
    }

    public Path path(String ecName) {

        if (ecName == null
                || !ecName.matches("[a-zA-Z0-9][a-zA-Z0-9._-]*")) {

            throw new IllegalArgumentException(
                    "Invalid EC identity: " + ecName
            );
        }

        return home.resolve("run")
                .resolve(ecName + ".process.properties");
    }

    public Optional<Identity> read(String ecName) throws IOException {

        Path file = path(ecName);

        if (!Files.exists(file)) {
            return Optional.empty();
        }

        Properties values = new Properties();

        try (var input = Files.newInputStream(file)) {
            values.load(input);
        }

        try {

            Identity identity = new Identity(
                    Long.parseLong(values.getProperty("pid")),
                    Instant.parse(values.getProperty("startedAt")),
                    values.getProperty("containerId"),
                    values.getProperty("applicationHome")
            );

            if (identity.pid() <= 0
                    || !ecName.equals(identity.containerId())
                    || !home.toString().equals(identity.applicationHome())) {

                throw new IllegalArgumentException(
                        "EC identity does not match this installation"
                );
            }

            return Optional.of(identity);

        } catch (RuntimeException exception) {

            throw new IOException(
                    "Invalid EC identity file: " + file,
                    exception
            );
        }
    }

    public void write(
            String ecName,
            ProcessHandle process
    ) throws IOException {

        Instant start = process.info()
                .startInstant()
                .orElseThrow(
                        () -> new IOException(
                                "Cannot verify process start time for PID "
                                        + process.pid()
                        )
                );

        Properties values = new Properties();

        values.setProperty(
                "pid",
                Long.toString(process.pid())
        );

        values.setProperty(
                "startedAt",
                start.toString()
        );

        values.setProperty(
                "containerId",
                ecName
        );

        values.setProperty(
                "applicationHome",
                home.toString()
        );

        Path target = path(ecName);

        Files.createDirectories(target.getParent());

        Path temporary = Files.createTempFile(
                target.getParent(),
                ecName + "-",
                ".tmp"
        );

        try {

            try (var output = Files.newOutputStream(temporary)) {

                values.store(
                        output,
                        "EC process identity; verify PID, start time "
                                + "and JVM arguments before adoption"
                );
            }

            try {

                Files.move(
                        temporary,
                        target,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING
                );

            } catch (AtomicMoveNotSupportedException exception) {

                throw new IOException(
                        "EC identity directory must support atomic replacement: "
                                + target,
                        exception
                );
            }

        } finally {

            Files.deleteIfExists(temporary);
        }
    }
}
