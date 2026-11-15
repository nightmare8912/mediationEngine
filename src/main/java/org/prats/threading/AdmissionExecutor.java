
package org.prats.threading;

import org.prats.loggers.EngineLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounds queued plus running work, including virtual threads.
 * Never waits for capacity.
 */
public final class AdmissionExecutor extends AbstractExecutorService {

    private static final EngineLogger log =
            EngineLogger.getLogger(AdmissionExecutor.class);

    private final ExecutorService delegate;
    private final Semaphore permits;
    private final int limit;

    private final AtomicLong rejected = new AtomicLong();

    public AdmissionExecutor(
            ExecutorService delegate,
            int limit
    ) {

        if (limit < 1) {
            throw new IllegalArgumentException(
                    "Admission limit must be positive"
            );
        }

        this.delegate = delegate;
        this.limit = limit;
        this.permits = new Semaphore(limit);

        log.debug(() ->
                "[AdmissionExecutor][constructor] Bounded worker pool initialized"
                        + " | limit=" + limit
        );
    }

    private final class Task implements Runnable {

        private final Runnable command;

        private final AtomicBoolean released =
                new AtomicBoolean();

        private Task(Runnable command) {
            this.command = command;
        }

        void release() {

            if (released.compareAndSet(false, true)) {
                permits.release();
            }
        }

        @Override
        public void run() {

            try {
                command.run();
            } finally {
                release();
            }
        }
    }

    @Override
    public void execute(Runnable command) {

        Objects.requireNonNull(command);

        if (!permits.tryAcquire()) {

            long count = rejected.incrementAndGet();

            if ((count & (count - 1)) == 0) {

                log.warn(
                        "[AdmissionExecutor][execute] Service admission rejected work"
                                + " | limit=" + limit
                                + " | totalRejected=" + count
                );
            }

            throw new RejectedExecutionException(
                    "Service admission limit reached: " + limit
            );
        }

        Task task = new Task(command);

        try {

            delegate.execute(task);

        } catch (RuntimeException e) {

            task.release();

            rejected.incrementAndGet();

            throw e;
        }
    }

    public Map<String, Object> metrics() {

        return Map.of(
                "admissionLimit",
                limit,

                "admittedTasks",
                limit - permits.availablePermits(),

                "admissionRejections",
                rejected.get()
        );
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {

        List<Runnable> original = new ArrayList<>();

        for (Runnable runnable : delegate.shutdownNow()) {

            if (runnable instanceof AdmissionExecutor.Task task) {

                task.release();

                original.add(task.command);

                if (task.command instanceof Future<?> future) {
                    future.cancel(false);
                }
            }
        }

        return original;
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(
            long time,
            TimeUnit unit
    ) throws InterruptedException {

        return delegate.awaitTermination(time, unit);
    }
}
