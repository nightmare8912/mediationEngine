package org.prats.threading;

import java.util.Map;
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
        ExecutorService delegate;
        if (createPlatformThread)
            delegate = initializeAndGetPlatformThreadPool(
                    coreThreads, maxThreads, idleTimeout
            );
        else
            delegate = initializeAndGetVirtualThreadPool();

        return new AdmissionExecutor(delegate, Integer.getInteger("mediation.admission.max-tasks", 1024));

    }
    public static Map<String, Object> admissionMetrics(ExecutorService executor) {
        return executor instanceof AdmissionExecutor bounded ? bounded.metrics() : Map.of();
    }
}
