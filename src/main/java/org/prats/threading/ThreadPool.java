package org.prats.threading;

import java.util.concurrent.*;

public class ThreadPool {

    public ExecutorService initializeAndGetThreadPool(int coreThreads, int maxThreads, long idleTimeout) {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
