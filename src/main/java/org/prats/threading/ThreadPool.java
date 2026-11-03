package org.prats.threading;

import java.util.concurrent.*;

public class ThreadPool {

    public ExecutorService initializeAndGetThreadPool(int coreThreads, int maxThreads, long idleTimeout) {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    // added it back in case we need it in future?
    public ExecutorService initializeAndGetThreadPool(
            int coreThreads,
            int maxThreads,
            long idleTimeout,
            boolean createRealThread
    ) {
        return new ThreadPoolExecutor(
                coreThreads,
                maxThreads,
                idleTimeout,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }
}
