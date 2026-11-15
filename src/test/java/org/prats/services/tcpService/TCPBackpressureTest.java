package org.prats.services.tcpService;

import org.junit.jupiter.api.*;
import org.prats.testing.TestDatabase;
import org.prats.definitions.classes.*;
import org.prats.definitions.interfaces.TCPProcessor;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class TCPBackpressureTest {
    @BeforeAll static void context(){TestDatabase.context();}
    private final TCPProcessor processor=new TCPProcessor(){
        public void processIncoming(String message,ClientConnection connection){}
        public String serialize(Message message){return "";}
        public CompletionStage<Void> processOutgoing(Message message,ClientConnection connection){return CompletableFuture.completedFuture(null);}
        public void informShutdown(){}
        public void cleanupCorrelations(){}
        public void reconcilePeers(){}
        public boolean isPeerRegistered(String peerId){return false;}
    };
    @Test void oversizedOutgoingMessagesAreRejectedWithoutRetention() throws Exception {
        TCPService service=new TCPService(1,1,60,15000,50,false,processor);
        var result=service.writeMessage("x".repeat(9*1024*1024),null);
        assertThrows(ExecutionException.class,()->result.get(1,TimeUnit.SECONDS));
        assertEquals(0,service.checkHealth().metrics().get("outboundRetainedBytes"));
    }
    @Test void stalledReadersAreClosedAndWriteMemoryIsReleased() throws Exception {
        String old=System.getProperty("mediation.tcp.write-timeout-seconds");
        System.setProperty("mediation.tcp.write-timeout-seconds","1");
        TCPService service=new TCPService(1,1,60,15000,50,false,processor);
        try(var listener=new java.net.ServerSocket(0);var receiver=new java.net.Socket()) {
            receiver.setReceiveBufferSize(1024);receiver.connect(new java.net.InetSocketAddress("127.0.0.1",listener.getLocalPort()));
            try(var sender=listener.accept()) {
                sender.setSendBufferSize(1024);var connection=new ClientConnection(sender);service.start();
                var result=service.writeMessage("x".repeat(2*1024*1024),connection);
                assertThrows(ExecutionException.class,()->result.get(4,TimeUnit.SECONDS));
                for(int i=0;i<100&&!sender.isClosed();i++)Thread.sleep(10);
                assertTrue(sender.isClosed());assertEquals(0,service.checkHealth().metrics().get("outboundRetainedBytes"));
            }
        }finally{service.shutdown();if(old==null)System.clearProperty("mediation.tcp.write-timeout-seconds");else System.setProperty("mediation.tcp.write-timeout-seconds",old);}
    }
}
