package org.prats.registries;
import org.junit.jupiter.api.Test;
import org.prats.definitions.interfaces.ReplyChannel;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
class RegistryCapacityTest {
    @org.junit.jupiter.api.BeforeAll static void context(){org.prats.testing.TestDatabase.context();}
    private final ReplyChannel reply=message->CompletableFuture.completedFuture(null);
    @Test void correlationCapacityIsStrictAndCleanupRestoresPermits() {
        CorrelationRegistry registry=new CorrelationRegistry(2); UUID first=UUID.randomUUID(),second=UUID.randomUUID();
        registry.add(first,reply);registry.add(second,reply);
        assertThrows(RejectedExecutionException.class,()->registry.add(UUID.randomUUID(),reply));
        registry.removeAndGet(first);registry.resetTimeout(first);assertEquals(1,registry.getSize());
        registry.add(UUID.randomUUID(),reply);assertEquals(2,registry.removeExpired(0).size());
        registry.add(UUID.randomUUID(),reply);registry.add(UUID.randomUUID(),reply);assertEquals(2,registry.getSize());
    }
    @Test void controlExpirationAlsoReleasesPermits() {
        TCPControlRegistry registry=new TCPControlRegistry(1);registry.add(UUID.randomUUID(),reply);
        assertThrows(RejectedExecutionException.class,()->registry.add(UUID.randomUUID(),reply));
        assertEquals(1,registry.removeExpired(0).size());registry.add(UUID.randomUUID(),reply);
        assertEquals(1,registry.getSize());
    }
    @Test void concurrentTrafficCannotExceedCapacity() throws Exception {
        CorrelationRegistry registry=new CorrelationRegistry(32);
        try(var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> tasks=new ArrayList<>();
            for(int i=0;i<1000;i++)tasks.add(executor.submit(()->{try{registry.add(UUID.randomUUID(),reply);}catch(RejectedExecutionException ignored){}}));
            for(var task:tasks)task.get();
        }
        assertEquals(32,registry.getSize());assertEquals(968,registry.rejectedCount());
        registry.removeExpired(0);assertEquals(0,registry.getSize());
    }
    @Test void retryPayloadBudgetIsReleasedWhenDeliveryIsRemoved() {
        String old=System.getProperty("mediation.retry.max-bytes");
        System.setProperty("mediation.retry.max-bytes","2500");
        try {
            RetryRegistry registry=new RetryRegistry(5,300);
            var parser=new org.prats.parsers.MessageParser(org.prats.definitions.enums.ProcessType.PLATFORM);
            var first=new org.prats.definitions.classes.PendingDelivery(UUID.randomUUID(),parser.createResponse(UUID.randomUUID(),Map.of("body","one")),reply);
            var second=new org.prats.definitions.classes.PendingDelivery(UUID.randomUUID(),parser.createResponse(UUID.randomUUID(),Map.of("body","two")),reply);
            var third=new org.prats.definitions.classes.PendingDelivery(UUID.randomUUID(),parser.createResponse(UUID.randomUUID(),Map.of("body","three")),reply);
            assertTrue(registry.add(first));assertTrue(registry.add(second));assertFalse(registry.add(third));
            assertTrue(registry.pendingBytes()<=2500);registry.remove(first.getCorrelationId(),first);assertTrue(registry.add(third));
            registry.remove(second.getCorrelationId(),second);registry.remove(third.getCorrelationId(),third);assertEquals(0,registry.pendingBytes());
        }finally{if(old==null)System.clearProperty("mediation.retry.max-bytes");else System.setProperty("mediation.retry.max-bytes",old);}
    }
    @Test void disconnectRemovesCorrelationsAndRestoresAdmission() throws Exception {
        try(var server=new java.net.ServerSocket(0);var client=new java.net.Socket("127.0.0.1",server.getLocalPort());var accepted=server.accept()) {
            var connection=new org.prats.definitions.classes.ClientConnection(accepted);
            var channel=new org.prats.replyChannels.TCPReplyChannel(null,connection);
            CorrelationRegistry registry=new CorrelationRegistry(1);registry.add(UUID.randomUUID(),channel);
            registry.removeForConnection(connection);assertEquals(0,registry.getSize());registry.add(UUID.randomUUID(),reply);
            accepted.close();assertThrows(RejectedExecutionException.class,()->new CorrelationRegistry(1).add(UUID.randomUUID(),channel));
        }
    }
}
