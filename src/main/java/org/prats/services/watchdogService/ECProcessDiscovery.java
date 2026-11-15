
package org.prats.services.watchdogService;

import org.prats.loggers.EngineLogger;
import org.prats.utilities.ECProcessIdentityStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Find surviving execution containers by application identity,
 * never by a bare persisted PID.
 */
final class ECProcessDiscovery {

    private static final String EC_MAIN =
            "org.prats.container.initializers.ExecutionContainerInitializer";

    private static final EngineLogger log =
            EngineLogger.getLogger(ECProcessDiscovery.class);


    // =========================================================
    // 1. EC PROCESS DISCOVERY
    // =========================================================

    static Optional<ProcessHandle> find(
            String ecName,
            Path applicationHome
    ) {

        ECProcessIdentityStore store =
                new ECProcessIdentityStore(applicationHome);

        Optional<ProcessHandle> recorded = Optional.empty();


        // -----------------------------------------------------
        // 1.1. Verify persisted EC process identity
        // -----------------------------------------------------

        try {

            recorded = store.read(ecName)
                    .flatMap(identity ->
                            ProcessHandle.of(identity.pid())
                                    .filter(identity::matches)
                                    .filter(process ->
                                            process.info()
                                                    .arguments()
                                                    .map(args ->
                                                            matches(
                                                                    args,
                                                                    ecName,
                                                                    applicationHome
                                                            )
                                                    )
                                                    .orElse(false)
                                    )
                    );

        } catch (IOException exception) {

            log.warn(
                    "[ECProcessDiscovery][find] Ignoring unreadable or invalid identity file"
                            + " | ecName=" + ecName
                            + " | reason=" + exception.getMessage()
            );
        }


        // -----------------------------------------------------
        // 1.2. Discover running EC processes
        // -----------------------------------------------------

        // Also detect duplicates and recover ECs launched
        // before identity files existed.

        List<ProcessHandle> matches;

        try (var processes = ProcessHandle.allProcesses()) {

            matches = processes
                    .filter(ProcessHandle::isAlive)
                    .filter(process ->
                            process.info()
                                    .arguments()
                                    .map(args ->
                                            matches(
                                                    args,
                                                    ecName,
                                                    applicationHome
                                            )
                                    )
                                    .orElse(false)
                    )
                    .toList();
        }


        // -----------------------------------------------------
        // 1.3. Reject ambiguous EC ownership
        // -----------------------------------------------------

        if (matches.size() > 1) {

            throw new IllegalStateException(
                    "Multiple EC processes match " + ecName
                            + "; refusing to launch or stop an ambiguous process."
            );
        }


        // -----------------------------------------------------
        // 1.4. Resolve verified process
        // -----------------------------------------------------

        Optional<ProcessHandle> discovered =
                matches.stream().findFirst();

        Optional<ProcessHandle> result =
                discovered.isPresent()
                        ? discovered
                        : recorded;


        // -----------------------------------------------------
        // 1.5. Persist adopted EC process identity
        // -----------------------------------------------------

        result.ifPresent(process -> {

            try {

                store.write(ecName, process);

                log.info(
                        "[ECProcessDiscovery][find] Verified EC process identity"
                                + " | ecName=" + ecName
                                + " | pid=" + process.pid()
                                + " | identityFile=" + store.path(ecName)
                );

            } catch (IOException exception) {

                log.warn(
                        "[ECProcessDiscovery][find] Unable to persist adopted EC identity"
                                + " | ecName=" + ecName
                                + " | reason=" + exception.getMessage()
                );
            }
        });

        return result;
    }


    // =========================================================
    // 2. PROCESS ARGUMENT VERIFICATION
    // =========================================================

    static boolean matches(
            String[] arguments,
            String ecName,
            Path applicationHome
    ) {

        List<String> args = Arrays.asList(arguments);

        int classPath = args.indexOf("-cp");

        if (classPath < 0
                || classPath + 2 >= args.size()
                || !EC_MAIN.equals(args.get(classPath + 2))) {

            return false;
        }

        return args.contains(
                "-Dmediation.home="
                        + applicationHome.toAbsolutePath().normalize()
        )
                && args.contains("-Dprocess.name=" + ecName)
                && args.contains("--container-name=" + ecName)
                && args.contains("--container-type=EC");
    }
}
