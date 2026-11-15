package org.prats.threading;
import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
class AdmissionExecutorTest {
    private void awaitReleased(AdmissionExecutor executor) {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
        while(!executor.metrics().get("admittedTasks").equals(0) && System.nanoTime()<deadline)
            java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        assertEquals(0,executor.metrics().get("admittedTasks"));
    }
    @Test void saturationRejectsWithoutGrowingAndRecoversAfterCompletion() throws Exception {
        AdmissionExecutor executor=new AdmissionExecutor(Executors.newVirtualThreadPerTaskExecutor(),2);
        CountDownLatch started=new CountDownLatch(2), release=new CountDownLatch(1);
        try {
            Callable<Void> work=()->{ assertTrue(Thread.currentThread().isVirtual());started.countDown();release.await();return null; };
            Future<?> first=executor.submit(work), second=executor.submit(work);
            assertTrue(started.await(3,TimeUnit.SECONDS));
            assertThrows(RejectedExecutionException.class,()->executor.submit(()->{}));
            assertEquals(2,executor.metrics().get("admittedTasks")); release.countDown();first.get();second.get();awaitReleased(executor);
            for(int i=0;i<1000;i++){executor.submit(()->{}).get();awaitReleased(executor);}
            assertEquals(0,executor.metrics().get("admittedTasks"));
            executor.submit(()->{throw new IllegalStateException("test");});
        } finally {release.countDown();executor.shutdownNow();assertTrue(executor.awaitTermination(3,TimeUnit.SECONDS));}
    }
    @Test void shutdownReleasesQueuedPermits() throws Exception {
        AdmissionExecutor executor=new AdmissionExecutor(Executors.newSingleThreadExecutor(),3);
        CountDownLatch started=new CountDownLatch(1);
        executor.submit(()->{started.countDown();try{Thread.sleep(60000);}catch(InterruptedException e){Thread.currentThread().interrupt();}});
        assertTrue(started.await(3,TimeUnit.SECONDS)); executor.submit(()->{});executor.submit(()->{});
        executor.shutdownNow();assertTrue(executor.awaitTermination(3,TimeUnit.SECONDS));
        assertEquals(0,executor.metrics().get("admittedTasks"));
    }
}
