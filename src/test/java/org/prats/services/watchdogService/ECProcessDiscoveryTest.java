package org.prats.services.watchdogService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.prats.utilities.ECProcessIdentityStore;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import javax.tools.ToolProvider;
import static org.junit.jupiter.api.Assertions.*;

class ECProcessDiscoveryTest {
    @TempDir Path home;
    private static final String MAIN = "org.prats.container.initializers.ExecutionContainerInitializer";

    @Test
    void discoveryRequiresInstallationNameAndEcMainClass() {
        String[] args = {"-Dmediation.home=" + home, "-Dprocess.name=ec01", "-cp", "engine.jar", MAIN,
                "--container-name=ec01", "--container-type=EC"};
        assertTrue(ECProcessDiscovery.matches(args, "ec01", home));
        assertFalse(ECProcessDiscovery.matches(args, "ec02", home));
        assertFalse(ECProcessDiscovery.matches(args, "ec01", home.resolve("other")));
        args[4] = "some.other.Main";
        assertFalse(ECProcessDiscovery.matches(args, "ec01", home));
    }

    @Test
    void orphanedEcIsRediscoveredWithTheSameIdentity() throws Exception {
        // A small fixture JVM uses the EC entry-point name, without opening any service ports.
        Path classes = home.resolve("classes");
        Files.createDirectories(classes);
        Path source = home.resolve("ExecutionContainerInitializer.java");
        Path mainClasses = Path.of(ECProcessIdentityStore.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Files.writeString(source, """
                package org.prats.container.initializers;
                public class ExecutionContainerInitializer {
                    public static void main(String[] args) throws Exception {
                        new org.prats.utilities.ECProcessIdentityStore(java.nio.file.Path.of(System.getProperty("mediation.home")))
                            .write(System.getProperty("process.name"), ProcessHandle.current());
                        Thread.sleep(60000);
                    }
                }
                """);
        Path parentSource = home.resolve("Parent.java");
        Files.writeString(parentSource, """
                public class Parent {
                    public static void main(String[] args) throws Exception {
                        Process child = new ProcessBuilder(args)
                            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                            .redirectError(ProcessBuilder.Redirect.DISCARD).start();
                        System.out.println(child.pid());
                    }
                }
                """);
        assertEquals(0, ToolProvider.getSystemJavaCompiler().run(null, null, null,
                "-classpath", mainClasses.toString(), "-d", classes.toString(), source.toString(), parentSource.toString()));
        String javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process parent = new ProcessBuilder(javaExecutable, "-cp", classes.toString(), "Parent", javaExecutable,
                "-Dmediation.home=" + home, "-Dprocess.name=ec01", "-cp",
                classes + java.io.File.pathSeparator + mainClasses, MAIN,
                "--container-name=ec01", "--container-type=EC").start();
        ProcessHandle child = null;
        try {
            String pid = parent.inputReader().readLine();
            assertTrue(parent.waitFor(5, TimeUnit.SECONDS));
            assertEquals(0, parent.exitValue());
            child = ProcessHandle.of(Long.parseLong(pid)).orElseThrow();
            ECProcessIdentityStore store = new ECProcessIdentityStore(home);
            for (int attempt = 0; attempt < 100 && !Files.exists(store.path("ec01")); attempt++) {
                Thread.sleep(25);
            }
            assertTrue(child.isAlive(), "Parent exit must leave EC running");
            assertTrue(store.read("ec01").orElseThrow().matches(child));
            assertEquals(child.pid(), ECProcessDiscovery.find("ec01", home).orElseThrow().pid());
            assertEquals(child.pid(), ECProcessDiscovery.find("ec01", home).orElseThrow().pid());
            Files.writeString(store.path("ec01"), "corrupt identity");
            assertEquals(child.pid(), ECProcessDiscovery.find("ec01", home).orElseThrow().pid(),
                    "Discovery must recover a valid EC even with a stale or corrupt record");
        } finally {
            parent.destroyForcibly();
            if (child != null) {
                child.destroyForcibly();
                child.onExit().get(5, TimeUnit.SECONDS);
            }
        }
    }
}
