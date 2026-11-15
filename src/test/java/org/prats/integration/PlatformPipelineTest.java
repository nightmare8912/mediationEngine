package org.prats.integration;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class PlatformPipelineTest {
    @TempDir(cleanup=org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS) Path home;
    private final ObjectMapper json=new ObjectMapper();
    private Process platform,watchdog;
    private ProcessHandle ec;
    private int appPort,controlPort,ecPort,ec2Port;
    private static int port() throws IOException {try(var server=new ServerSocket(0)){return server.getLocalPort();}}
    private Process launch(String name,String main,List<String> args) throws IOException {
        List<String> command=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin","java").toString(),
            "-Xms32m","-Xmx128m","-Dmediation.home="+home,"-Dprocess.name="+name,"-Dmediation.process.name="+name,
            "-Dmediation.log.directory="+home.resolve("logs"),"-cp",home.resolve("mediation-engine.jar").toString(),main));
        command.addAll(args);
        return new ProcessBuilder(command).directory(home.toFile()).redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.appendTo(home.resolve(name+".console.log").toFile())).start();
    }
    private void prepare() throws Exception {
        appPort=port();controlPort=port();ecPort=port();ec2Port=port();
        Files.createDirectories(home.resolve("logs"));Files.createDirectories(home.resolve("data"));
        Path configs=Path.of("src/main/resources/configs");
        try(var paths=Files.walk(configs)){
            for(Path source:paths.filter(Files::isRegularFile).filter(path -> path.toString().endsWith(".conf")).toList()){
                Path target=home.resolve("conf").resolve(configs.relativize(source));Files.createDirectories(target.getParent());
                String content=Files.readString(source).replace("15000",Integer.toString(appPort)).replace("15001",Integer.toString(controlPort))
                    .replace("5100",Integer.toString(ecPort)).replace("10000",Integer.toString(ec2Port))
                    .replace("-Xms256m","-Xms32m").replace("-Xmx768m","-Xmx128m");
                Files.writeString(target,content);
            }
        }
        Files.copy(Path.of("build/libs/mediation-engine.jar"),home.resolve("mediation-engine.jar"));
    }
    private void waitForPlatform() throws Exception {
        for(int i=0;i<100;i++){
            if(!platform.isAlive())fail(Files.readString(home.resolve("platform.console.log")));
            try(var socket=new Socket("127.0.0.1",appPort)){return;}catch(IOException e){Thread.sleep(100);}
        }fail("Platform did not bind its test port");
    }
    private final class Client implements AutoCloseable {
        final Socket socket=new Socket("127.0.0.1",appPort);
        final BufferedReader input=new BufferedReader(new InputStreamReader(socket.getInputStream(),java.nio.charset.StandardCharsets.UTF_8));
        final BufferedWriter output=new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(),java.nio.charset.StandardCharsets.UTF_8));
        Client() throws IOException {socket.setSoTimeout(40000);}
        JsonNode request(String action,Map<String,?> values) throws Exception {
            ObjectNode payload=json.valueToTree(values);payload.put("action",action);
            ObjectNode message=json.createObjectNode();message.put("messageId",UUID.randomUUID().toString());message.put("correlationId",UUID.randomUUID().toString());
            message.put("messageType","REQUEST");message.put("processType","GUI");message.put("operation","GUI");message.put("timestamp",Instant.now().toString());message.set("payload",payload);
            output.write(json.writeValueAsString(message));output.newLine();output.flush();
            String line=input.readLine();assertNotNull(line,"Platform disconnected");return json.readTree(line).path("payload");
        }
        JsonNode internalRouteAttempt(String operation, Map<String, ?> values) throws Exception {
            String correlation = UUID.randomUUID().toString();
            ObjectNode message = json.createObjectNode();
            message.put("messageId", UUID.randomUUID().toString());
            message.put("correlationId", correlation);
            message.put("messageType", "REQUEST");
            message.put("processType", "PLATFORM");
            message.put("operation", operation);
            message.put("timestamp", Instant.now().toString());
            message.put("accountDatabaseRequest", true);
            message.set("payload", json.valueToTree(values));
            output.write(json.writeValueAsString(message)); output.newLine(); output.flush();
            JsonNode response = json.readTree(input.readLine());
            assertEquals(correlation, response.path("correlationId").asText());
            return response.path("payload");
        }
        String text(String command) throws IOException {output.write(command);output.newLine();output.flush();return input.readLine();}
        void login() throws Exception {assertTrue(request("login",Map.of("username","test-user","password","Secure-test-123!")).path("ok").asBoolean());}
        public void close() throws IOException{socket.close();}
    }
    @Test @Timeout(150)
    void guiAndNcAccountsDiagnosticsAndWatchdogRecoveryUseTheRealPipeline() throws Exception {
        prepare();
        try {
            platform=launch("platform","org.prats.container.initializers.PlatformInitializer",List.of(
                "--tcp-port="+appPort,"--control-port="+controlPort,"--tcp-backlog=50","--core-threads=5","--max-threads=10","--idle-timeout=60",
                "--database-conf="+home.resolve("conf/databaseConfigs/table_database.conf"),"--container-name=platform","--container-type=PLATFORM",
                "--container-conf="+home.resolve("conf/containerConfigs/platformConfigs/platform.conf")));
            waitForPlatform();
            watchdog=launch("wdg01","org.prats.container.initializers.WatchdogInitializer",List.of("--watchdog-conf="+home.resolve("conf/containerConfigs/watchdogConfigs/wdg1.conf")));
            try(Client gui=new Client()){
                assertFalse(gui.request("whoami",Map.of()).path("ok").asBoolean());
                assertTrue(gui.request("register",Map.of("username","test-user","password","Secure-test-123!")).path("ok").asBoolean());gui.login();
                assertEquals("Status OK.",gui.request("command",Map.of("command","mng status")).path("body").asText());
                JsonNode protectedTable = gui.internalRouteAttempt("FETCH", Map.of("entity", "users"));
                assertFalse(protectedTable.path("ok").asBoolean());
                assertEquals("PROTECTED_ENTITY", protectedTable.path("errorCode").asText());
                assertFalse(gui.internalRouteAttempt("AUTHENTICATE", Map.of("action", "whoami")).path("ok").asBoolean());
                JsonNode local=gui.request("details",Map.of("target","platform"));assertTrue(local.path("ok").asBoolean(),local.toString());
                assertTrue(local.path("details").has("heap"));assertTrue(local.path("details").has("connections"));assertTrue(local.path("details").has("machine"));
                boolean linked=false;
                for(int i=0;i<40;i++){
                    JsonNode detail=gui.request("details",Map.of("target","wdg01"));
                    if(detail.path("ok").asBoolean()){linked=true;break;}Thread.sleep(500);
                }assertTrue(linked,"Watchdog did not reconnect: " + Files.readString(home.resolve("wdg01.console.log")));
                JsonNode started=gui.request("command",Map.of("command","mng start ec ec01"));assertTrue(started.path("body").asText().contains("Process started"),started.toString());
                Path identity=home.resolve("run/ec01.process.properties");for(int i=0;i<100&&!Files.exists(identity);i++)Thread.sleep(50);
                Properties properties=new Properties();try(var in=Files.newInputStream(identity)){properties.load(in);}
                long pid=Long.parseLong(properties.getProperty("pid"));ec=ProcessHandle.of(pid).orElseThrow();assertTrue(ec.isAlive());
                watchdog.destroyForcibly();assertTrue(watchdog.waitFor(5,TimeUnit.SECONDS));assertTrue(ec.isAlive(),"Watchdog exit must not terminate EC");
                watchdog=launch("wdg01","org.prats.container.initializers.WatchdogInitializer",List.of("--watchdog-conf="+home.resolve("conf/containerConfigs/watchdogConfigs/wdg1.conf")));
                Thread.sleep(17000);
                JsonNode duplicate=gui.request("command",Map.of("command","mng start ec ec01"));assertTrue(duplicate.path("body").asText().contains("already running"),duplicate.toString());
                assertEquals(pid,ec.pid());assertTrue(ec.isAlive());
                JsonNode details=gui.request("details",Map.of("target","ec01"));assertTrue(details.path("ok").asBoolean(),details.toString());assertEquals(pid,details.path("details").path("pid").asLong());
                assertTrue(gui.request("gc",Map.of("target","ec01")).path("details").path("gcRequested").asBoolean());
                assertEquals(4,gui.request("overview",Map.of()).path("jvms").size());
            }
            try (org.prats.desktop.PlatformClient desktop = new org.prats.desktop.PlatformClient()) {
                desktop.connect("127.0.0.1", appPort).get(10, TimeUnit.SECONDS);
                desktop.request("login", Map.of("username", "test-user", "password", "Secure-test-123!")).get(10, TimeUnit.SECONDS);
                assertEquals("Status OK.", desktop.request("command", Map.of("command", "mng status")).get(10, TimeUnit.SECONDS).path("body").asText());
                assertTrue(desktop.request("details", Map.of("target", "platform")).get(10, TimeUnit.SECONDS).path("details").has("heap"));
                assertEquals("test-user", desktop.request("whoami", Map.of()).get(10, TimeUnit.SECONDS).path("username").asText());
                desktop.request("logout", Map.of()).get(10, TimeUnit.SECONDS);
                assertThrows(java.util.concurrent.ExecutionException.class,
                        () -> desktop.request("whoami", Map.of()).get(10, TimeUnit.SECONDS));
            }
            try(Client nc=new Client()){
                assertTrue(nc.text("mng status").contains("Login required"));
                assertTrue(nc.text("mng register nc-operator Secure-test-123!").contains("User registered"));
                assertTrue(nc.text("mng login test-user Secure-test-123!").contains("Logged in as test-user"));
                assertEquals("Status OK.",nc.text("mng status"));assertTrue(nc.text("mng logout").contains("Logged out"));
            }
            String log=Files.readString(home.resolve("logs/platform-detail.log"));
            assertFalse(log.contains("Secure-test-123!"),"Passwords must never appear in platform logs");
            assertTrue(log.contains("GUIForwarder"));assertTrue(log.contains("GUIService"));assertTrue(log.contains("GUIHandler"));
            assertTrue(Files.readString(home.resolve("logs/wdg01-detail.log")).contains("Reattached to surviving EC"));
        } finally {
            if(ec!=null){ec.destroyForcibly();ec.onExit().get(5,TimeUnit.SECONDS);}
            if(watchdog!=null){watchdog.destroyForcibly();watchdog.waitFor(5,TimeUnit.SECONDS);}
            if(platform!=null){platform.destroyForcibly();platform.waitFor(5,TimeUnit.SECONDS);}
        }
    }
}
