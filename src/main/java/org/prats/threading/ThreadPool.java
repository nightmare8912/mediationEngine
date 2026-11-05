package org.prats.threading;

import java.util.concurrent.*;

public class ThreadPool {

    private ExecutorService initializeAndGetVirtualThreadPool() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    // added it back in case we need it in future?
    private ExecutorService initializeAndGetPlatformThreadPool(
            int coreThreads,
            int maxThreads,
            long idleTimeout
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

    public ExecutorService initializeAndGetThreadPool(
            int coreThreads,
            int maxThreads,
            long idleTimeout,
            boolean createPlatformThread
    ) {
        if (createPlatformThread)
            return initializeAndGetPlatformThreadPool(
                    coreThreads, maxThreads, idleTimeout
            );
        else
            return initializeAndGetVirtualThreadPool();

    }
}
