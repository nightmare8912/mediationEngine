package org.prats.handlers;

import org.junit.jupiter.api.*;
import org.prats.testing.TestDatabase;
import org.prats.testing.TestMessagePipeline;
import org.prats.parsers.MessageParser;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.*;
import org.prats.definitions.interfaces.MessageDispatcher;
import org.prats.security.*;
import org.prats.services.guiService.GUIService;
import org.prats.forwarders.GUIForwarder;
import org.prats.processors.MessageProcessor;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class GUIServiceTest {
    @BeforeAll static void context(){TestDatabase.context();}
    private final MessageParser parser=new MessageParser(ProcessType.PLATFORM);
    private Message request(String action,Map<String,?> values,ClientSessionContext context) {
        Message request=parser.createEmptyMessage();request.setMessageType(MessageType.REQUEST);request.setOperation(Operation.GUI);
        var payload=parser.convertToJsonNode(values).deepCopy();((com.fasterxml.jackson.databind.node.ObjectNode)payload).put("action",action);
        request.setPayload(payload);request.setClientSession(context);return request;
    }
    @Test void accountOperationsUseTheSameConnectionSessionAndEraseCredentials() {
        try (TestMessagePipeline pipeline = new TestMessagePipeline()) {
            ClientSessionContext context=new ClientSessionContext();
            assertFalse(pipeline.request(request("whoami",Map.of(),context)).join().getPayload().path("ok").asBoolean());
            Message register=request("register",Map.of("username","gui-user","password","Secure-pass-123!"),context);
            assertTrue(pipeline.request(register).join().getPayload().path("ok").asBoolean());
            assertFalse(register.getPayload().has("password"));
            assertTrue(pipeline.request(request("login",Map.of("username","gui-user","password","Secure-pass-123!"),context)).join().getPayload().path("ok").asBoolean());
            assertEquals("gui-user",pipeline.request(request("whoami",Map.of(),context)).join().getPayload().path("username").asText());
            pipeline.request(request("logout",Map.of(),context)).join();
            assertNull(context.token());
            assertEquals(0, pipeline.correlations.getSize());
        }
    }

    @Test void guiAuthenticationAndStorageUseDistinctMessageCorrelations() {
        try (TestMessagePipeline pipeline = new TestMessagePipeline()) {
            ClientSessionContext context = new ClientSessionContext();
            Message register = request("register", Map.of("username", "route-user", "password", "Secure-pass-123!"), context);
            Message response = pipeline.request(register).join();
            assertTrue(response.getPayload().path("ok").asBoolean());
            assertEquals(register.getCorrelationId(), response.getCorrelationId());
            assertEquals(List.of(Operation.GUI, Operation.AUTHENTICATE, Operation.CREATE),
                    pipeline.routes.stream().map(TestMessagePipeline.Route::operation).toList());
            assertEquals(3, pipeline.routes.stream().map(TestMessagePipeline.Route::correlation).distinct().count());
            assertTrue(pipeline.routes.get(2).accountDatabase());
            pipeline.routes.clear();
            assertTrue(pipeline.request(request("login", Map.of("username", "route-user", "password", "Secure-pass-123!"), context))
                    .join().getPayload().path("ok").asBoolean());
            assertEquals(List.of(Operation.GUI, Operation.AUTHENTICATE, Operation.SEARCH),
                    pipeline.routes.stream().map(TestMessagePipeline.Route::operation).toList());
            pipeline.routes.clear();
            assertTrue(pipeline.request(request("containers", Map.of(), context)).join().getPayload().path("ok").asBoolean());
            assertEquals(List.of(Operation.GUI, Operation.AUTHENTICATE),
                    pipeline.routes.stream().map(TestMessagePipeline.Route::operation).toList());
        }
    }

    @Test void pendingHandlerDoesNotKeepItsInvocationWorkerWaiting() throws Exception {
        CompletableFuture<Message> pending = new CompletableFuture<>();
        CompletableFuture<Thread> invoked = new CompletableFuture<>();
        CompletableFuture<Message> delivered = new CompletableFuture<>();
        var handler = new org.prats.definitions.interfaces.AsyncRequestHandler() {
            @Override public CompletionStage<Message> handleRequestAsync(Message request) {
                invoked.complete(Thread.currentThread());
                return pending;
            }
        };
        MessageDispatcher dispatcher = new MessageDispatcher() {
            public void dispatchRequest(Message request, org.prats.definitions.interfaces.ReplyChannel reply) {}
            public void dispatchResponse(Message response) { delivered.complete(response); }
        };
        GUIService service = new GUIService(handler, dispatcher);
        service.start();
        try {
            Message request = request("containers", Map.of(), new ClientSessionContext());
            service.submit(request);
            Thread invocation = invoked.get(3, TimeUnit.SECONDS);
            invocation.join(1000);
            assertFalse(invocation.isAlive(), "Invocation worker must end while the reply is pending");
            assertFalse(delivered.isDone());
            pending.complete(request);
            assertSame(request, delivered.get(3, TimeUnit.SECONDS));
        } finally { service.shutdown(); }
    }

    @Test void closedConnectionCannotAcquireANewLoginSession() {
        try (TestMessagePipeline pipeline = new TestMessagePipeline()) {
            ClientSessionContext context = new ClientSessionContext();
            assertTrue(pipeline.request(request("register", Map.of("username", "closed-user", "password", "Secure-pass-123!"), context))
                    .join().getPayload().path("ok").asBoolean());
            context.close();
            Message response = pipeline.request(request("login", Map.of("username", "closed-user", "password", "Secure-pass-123!"), context)).join();
            assertFalse(response.getPayload().path("ok").asBoolean());
            assertNull(context.token());
        }
    }

    @Test void forwarderServiceHandlerAndReplyRunOnManagedVirtualThreads() throws Exception {
        CompletableFuture<Message> response=new CompletableFuture<>();
        MessageDispatcher dispatcher=new MessageDispatcher(){
            public void dispatchRequest(Message message,org.prats.definitions.interfaces.ReplyChannel reply){}
            public void dispatchResponse(Message message){response.complete(message);}
        };
        GUIService service=new GUIService(request->{assertTrue(Thread.currentThread().isVirtual());return request;},dispatcher);
        MessageProcessor processor=new MessageProcessor(null);processor.setGUIForwarder(new GUIForwarder(service));
        service.start();
        try {
            Message request=request("whoami",Map.of(),new ClientSessionContext());processor.process(request);
            assertSame(request,response.get(3,TimeUnit.SECONDS));assertEquals(HealthStatus.HEALTHY,service.checkHealth().status());
            service.quiesce();assertEquals(HealthStatus.UNREACHABLE,service.checkHealth().status());
        }finally{service.shutdown();}
    }
}
