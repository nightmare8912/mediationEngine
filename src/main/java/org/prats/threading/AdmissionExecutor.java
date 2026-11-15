
package org.prats.threading;

import org.prats.loggers.EngineLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bounded admission-controlled ExecutorService.
 *
 * <p>
 * AdmissionExecutor wraps an existing ExecutorService
 * and limits the total number of admitted tasks.
 * </p>
 *
 * <p>
 * The admission limit includes both:
 * </p>
 *
 * <ul>
 *     <li>Tasks currently executing.</li>
 *     <li>Tasks accepted but waiting in the delegate queue.</li>
 * </ul>
 *
 * <p>
 * This class works with platform-thread executors
 * and virtual-thread executors.
 * </p>
 *
 * <p>
 * Important design principles:
 * </p>
 *
 * <ol>
 *     <li>Never wait for available capacity.</li>
 *     <li>Reject immediately when the limit is reached.</li>
 *     <li>Release permits when tasks complete or fail.</li>
 *     <li>Release permits if the delegate rejects a task.</li>
 *     <li>Release permits for queued tasks returned by shutdownNow().</li>
 *     <li>Prevent double permit releases.</li>
 * </ol>
 *
 * <p>
 * AdmissionExecutor does not create or manage threads
 * directly. Thread creation and scheduling remain
 * responsibilities of the delegate ExecutorService.
 * </p>
 */
public final class AdmissionExecutor
        extends AbstractExecutorService {

    private static final EngineLogger log =
            EngineLogger.getLogger(AdmissionExecutor.class);

    // =========================================================
    // 1. EXECUTOR CONFIGURATION
    // =========================================================

    /**
     * Underlying executor responsible for scheduling
     * and executing admitted tasks.
     *
     * Examples:
     *
     * Executors.newVirtualThreadPerTaskExecutor()
     *
     * Executors.newFixedThreadPool(4)
     */
    private final ExecutorService delegate;

    /**
     * Controls admission capacity.
     *
     * Each accepted task consumes one permit.
     *
     * The permit remains occupied from admission
     * until the task completes or is discarded.
     */
    private final Semaphore permits;

    /**
     * Maximum number of admitted tasks.
     *
     * Includes both queued and running work.
     */
    private final int limit;

    // =========================================================
    // 2. EXECUTION METRICS
    // =========================================================

    /**
     * Total number of rejected submissions.
     *
     * Includes:
     *
     * 1. Admission-capacity rejections.
     * 2. Runtime failures or rejections during
     *    submission to the delegate executor.
     *
     * AtomicLong allows multiple threads to
     * update the counter safely.
     */
    private final AtomicLong rejected =
            new AtomicLong();

    // =========================================================
    // 3. CONSTRUCTOR
    // =========================================================

    /**
     * Creates an admission-controlled executor.
     *
     * @param delegate Underlying ExecutorService.
     * @param limit    Maximum number of admitted tasks.
     *
     * @throws NullPointerException
     *         if delegate is null.
     *
     * @throws IllegalArgumentException
     *         if limit is less than one.
     */
    public AdmissionExecutor(
            ExecutorService delegate,
            int limit
    ) {

        // -----------------------------------------------------
        // VALIDATE EXECUTOR
        // -----------------------------------------------------

        this.delegate = Objects.requireNonNull(
                delegate,
                "Delegate executor cannot be null"
        );

        // -----------------------------------------------------
        // VALIDATE ADMISSION LIMIT
        // -----------------------------------------------------

        if (limit < 1) {

            log.error(
                    "[AdmissionExecutor][constructor] "
                            + "Invalid admission limit"
                            + " | limit=" + limit
            );

            throw new IllegalArgumentException(
                    "Admission limit must be positive"
            );
        }

        // -----------------------------------------------------
        // INITIALIZE ADMISSION CONTROL
        // -----------------------------------------------------

        this.limit = limit;

        /*
         * A Semaphore initially contains 'limit'
         * available permits.
         *
         * We use tryAcquire() instead of acquire().
         *
         * This guarantees that a submitting thread
         * will not wait for admission capacity.
         *
         * The default non-fair semaphore is suitable
         * because submissions never block for permits.
         */
        this.permits = new Semaphore(limit);

        log.info(() ->
                "[AdmissionExecutor][constructor] "
                        + "Admission-controlled executor initialized"
                        + " | delegate="
                        + delegate.getClass().getSimpleName()
                        + " | admissionLimit=" + limit
                        + " | availablePermits="
                        + permits.availablePermits()
        );
    }

    // =========================================================
    // 4. INTERNAL TASK WRAPPER
    // =========================================================

    /**
     * Wraps each admitted Runnable.
     *
     * The wrapper owns the lifecycle of the
     * permit acquired during execute().
     *
     * Each task has an independent AtomicBoolean
     * that prevents its permit from being released
     * more than once.
     */
    private final class Task implements Runnable {

        /**
         * Original task submitted by the caller.
         */
        private final Runnable command;

        /**
         * Tracks whether the admission permit
         * has already been released.
         *
         * A task's permit may be released through
         * different lifecycle paths:
         *
         * - Normal completion.
         * - Exceptional completion.
         * - Delegate submission failure.
         * - shutdownNow() queue draining.
         *
         * compareAndSet() ensures only one path
         * actually releases the permit.
         */
        private final AtomicBoolean released =
                new AtomicBoolean(false);

        // -----------------------------------------------------
        // TASK CONSTRUCTOR
        // -----------------------------------------------------

        private Task(
                Runnable command
        ) {

            this.command = Objects.requireNonNull(
                    command,
                    "Task command cannot be null"
            );
        }

        // -----------------------------------------------------
        // RELEASE ADMISSION PERMIT
        // -----------------------------------------------------

        /**
         * Releases this task's permit at most once.
         *
         * This method is safe if multiple threads
         * attempt to release the same task.
         */
        private void release() {

            /*
             * Only the thread that successfully changes
             * released from false to true may return
             * the permit to the semaphore.
             */
            if (released.compareAndSet(false, true)) {

                permits.release();

                /*
                 * No per-task DEBUG logging here.
                 *
                 * This method executes for every
                 * completed task and may be called
                 * thousands of times per second.
                 */
            }
        }

        // -----------------------------------------------------
        // EXECUTE ORIGINAL TASK
        // -----------------------------------------------------

        @Override
        public void run() {

            try {

                /*
                 * Execute the original task.
                 *
                 * Any exception is allowed to propagate
                 * to the underlying executor.
                 *
                 * We must never swallow task failures.
                 */
                command.run();

            } finally {

                /*
                 * Always release the admission permit.
                 *
                 * This runs even if command.run()
                 * throws RuntimeException or Error.
                 *
                 * The finally block is essential:
                 * otherwise a failed task could
                 * permanently consume admission capacity.
                 */
                release();
            }
        }
    }

    // =========================================================
    // 5. TASK ADMISSION
    // =========================================================

    /**
     * Attempts to submit a task for execution.
     *
     * Admission is strictly non-blocking.
     *
     * If capacity is available:
     *
     * 1. Acquire one permit.
     * 2. Wrap the original task.
     * 3. Submit the wrapper to the delegate.
     *
     * If capacity is exhausted:
     *
     * 1. Increment the rejection counter.
     * 2. Log a rate-limited warning.
     * 3. Throw RejectedExecutionException.
     *
     * @param command Task to execute.
     *
     * @throws RejectedExecutionException
     *         if the admission limit is reached
     *         or the delegate rejects the task.
     */
    @Override
    public void execute(
            Runnable command
    ) {

        Objects.requireNonNull(
                command,
                "Submitted task cannot be null"
        );

        // -----------------------------------------------------
        // ATTEMPT PERMIT ACQUISITION
        // -----------------------------------------------------

        /*
         * tryAcquire() returns immediately:
         *
         * true  -> permit successfully acquired.
         * false -> no permits available.
         *
         * Unlike acquire(), this method never waits
         * for another task to complete.
         */
        if (!permits.tryAcquire()) {

            // -------------------------------------------------
            // REJECT WORK
            // -------------------------------------------------

            long count = rejected.incrementAndGet();

            /*
             * Log only when the rejection count is
             * a power of two.
             *
             * Examples:
             *
             * 1, 2, 4, 8, 16, 32, 64 ...
             *
             * This avoids flooding the error logs
             * during sustained overload.
             *
             * A positive number is a power of two
             * when:
             *
             * (count & (count - 1)) == 0
             */
            if ((count & (count - 1)) == 0) {

                log.warn(
                        "[AdmissionExecutor][execute] "
                                + "Task rejected: admission capacity exhausted"
                                + " | admissionLimit=" + limit
                                + " | availablePermits="
                                + permits.availablePermits()
                                + " | totalRejected=" + count
                );
            }

            throw new RejectedExecutionException(
                    "Service admission limit reached: " + limit
            );
        }

        // -----------------------------------------------------
        // WRAP ADMITTED TASK
        // -----------------------------------------------------

        /*
         * The permit is now reserved for this task.
         *
         * It remains reserved even if the delegate
         * queues the task rather than executing
         * it immediately.
         */
        Task task = new Task(command);

        // -----------------------------------------------------
        // SUBMIT TO DELEGATE EXECUTOR
        // -----------------------------------------------------

        try {

            /*
             * The delegate executor determines:
             *
             * - Which thread runs the task.
             * - When execution begins.
             * - Whether tasks are queued.
             * - How shutdown is handled.
             *
             * AdmissionExecutor only controls
             * whether the task is accepted.
             */
            delegate.execute(task);

        } catch (RuntimeException exception) {

            // -------------------------------------------------
            // ROLLBACK ADMISSION
            // -------------------------------------------------

            /*
             * If the delegate rejects the submission,
             * or fails before accepting it, release
             * the reserved permit.
             *
             * The AtomicBoolean prevents double
             * release if another path has already
             * released the permit.
             */
            task.release();

            long count = rejected.incrementAndGet();

            /*
             * A RejectedExecutionException commonly
             * occurs when the delegate is shutting down
             * or has its own capacity restrictions.
             *
             * Other runtime failures are logged as errors.
             */
            if (exception instanceof RejectedExecutionException) {

                if ((count & (count - 1)) == 0) {

                    log.warn(
                            "[AdmissionExecutor][execute] "
                                    + "Delegate executor rejected admitted task"
                                    + " | delegateShutdown="
                                    + delegate.isShutdown()
                                    + " | admissionLimit=" + limit
                                    + " | totalRejected=" + count
                    );
                }

            } else {

                log.error(
                        "[AdmissionExecutor][execute] "
                                + "Delegate executor failed during task submission"
                                + " | admissionLimit=" + limit
                                + " | totalRejected=" + count,
                        exception
                );
            }

            /*
             * Preserve the delegate's original exception.
             *
             * The calling service can decide whether
             * to retry, reject, or report a failure.
             */
            throw exception;
        }
    }

    // =========================================================
    // 6. ADMISSION METRICS
    // =========================================================

    /**
     * Returns a snapshot of admission metrics.
     *
     * admittedTasks represents the current number
     * of permits in use.
     *
     * It includes running and queued admitted tasks.
     *
     * This is not a cumulative task count.
     *
     * Metrics are collected concurrently and therefore
     * do not represent an atomic point-in-time snapshot
     * across every field.
     *
     * @return Immutable map containing admission metrics.
     */
    public Map<String, Object> metrics() {

        /*
         * The number of occupied permits is:
         *
         * admissionLimit - availablePermits
         */
        int admittedTasks =
                limit - permits.availablePermits();

        long totalRejected =
                rejected.get();

        return Map.of(
                "admissionLimit",
                limit,

                "admittedTasks",
                admittedTasks,

                "admissionRejections",
                totalRejected
        );
    }

    // =========================================================
    // 7. GRACEFUL SHUTDOWN
    // =========================================================

    /**
     * Initiates graceful shutdown.
     *
     * The delegate stops accepting new work but
     * attempts to finish already admitted tasks.
     *
     * This method does not wait for termination.
     *
     * Existing tasks continue to own their permits
     * until they complete.
     */
    @Override
    public void shutdown() {

        log.info(() ->
                "[AdmissionExecutor][shutdown] "
                        + "Graceful executor shutdown requested"
                        + " | admissionLimit=" + limit
                        + " | admittedTasks="
                        + (limit - permits.availablePermits())
                        + " | totalRejected=" + rejected.get()
        );

        delegate.shutdown();

        log.debug(
                "[AdmissionExecutor][shutdown] "
                        + "Shutdown request forwarded to delegate"
        );
    }

    // =========================================================
    // 8. IMMEDIATE SHUTDOWN
    // =========================================================

    /**
     * Attempts to stop executing tasks immediately.
     *
     * The delegate typically interrupts running
     * tasks and returns queued tasks that have
     * not yet started.
     *
     * IMPORTANT:
     *
     * Thread interruption is cooperative.
     *
     * This method cannot guarantee that already
     * running tasks will stop immediately.
     *
     * For queued tasks returned by the delegate:
     *
     * 1. Release their admission permits.
     * 2. Recover the original submitted Runnable.
     * 3. Cancel Future-based tasks where applicable.
     *
     * Running tasks release their own permits
     * when their run() method exits.
     *
     * @return Original queued tasks that never started.
     */
    @Override
    public List<Runnable> shutdownNow() {

        log.warn(
                "[AdmissionExecutor][shutdownNow] "
                        + "Immediate executor shutdown requested"
                        + " | admittedTasks="
                        + (limit - permits.availablePermits())
                        + " | admissionLimit=" + limit
        );

        // -----------------------------------------------------
        // REQUEST DELEGATE SHUTDOWN
        // -----------------------------------------------------

        /*
         * The delegate returns tasks that were queued
         * but did not begin execution.
         *
         * Because AdmissionExecutor wraps tasks,
         * the returned Runnables will normally
         * be instances of our internal Task class.
         */
        List<Runnable> notStarted =
                delegate.shutdownNow();

        List<Runnable> original =
                new ArrayList<>(notStarted.size());

        int recoveredTasks = 0;

        // -----------------------------------------------------
        // RECOVER ORIGINAL TASKS
        // -----------------------------------------------------

        for (Runnable runnable : notStarted) {

            if (runnable instanceof AdmissionExecutor.Task task) {

                /*
                 * This queued task will not execute,
                 * so its run() finally block will
                 * never release the permit.
                 *
                 * We must release it here.
                 */
                task.release();

                /*
                 * Return the original task to the caller,
                 * not the internal admission wrapper.
                 */
                original.add(task.command);

                recoveredTasks++;

                /*
                 * AbstractExecutorService.submit()
                 * commonly wraps submitted work
                 * in a FutureTask.
                 *
                 * Cancel that Future if its task
                 * will never execute.
                 *
                 * This prevents callers waiting
                 * indefinitely on an unfinished Future.
                 */
                if (task.command instanceof Future<?> future) {

                    future.cancel(false);
                }

            } else {

                /*
                 * Normally all queued tasks should
                 * be our wrappers.
                 *
                 * Preserve unexpected delegate tasks
                 * instead of silently discarding them.
                 */
                original.add(runnable);

                log.warn(
                        "[AdmissionExecutor][shutdownNow] "
                                + "Delegate returned an unrecognized task wrapper"
                                + " | taskType="
                                + runnable.getClass().getName()
                );
            }
        }

        final int finalRecoveredTasks =
                recoveredTasks;

        log.info(() ->
                "[AdmissionExecutor][shutdownNow] "
                        + "Immediate shutdown processing completed"
                        + " | recoveredQueuedTasks="
                        + finalRecoveredTasks
                        + " | totalReturnedTasks="
                        + original.size()
                        + " | remainingAdmittedTasks="
                        + (limit - permits.availablePermits())
        );

        return original;
    }

    // =========================================================
    // 9. SHUTDOWN STATUS
    // =========================================================

    /**
     * Returns true if shutdown has been initiated.
     *
     * This does not imply that all tasks have completed.
     */
    @Override
    public boolean isShutdown() {

        return delegate.isShutdown();
    }

    /**
     * Returns true once the delegate executor
     * has completely terminated.
     *
     * All running tasks must have exited.
     */
    @Override
    public boolean isTerminated() {

        return delegate.isTerminated();
    }

    // =========================================================
    // 10. WAIT FOR EXECUTOR TERMINATION
    // =========================================================

    /**
     * Waits for executor termination.
     *
     * This method may block the calling thread,
     * unlike the non-blocking admission mechanism.
     *
     * It is intended for lifecycle management,
     * not normal task submission.
     *
     * @param time Maximum time to wait.
     * @param unit Time unit.
     *
     * @return true if terminated before timeout,
     *         false otherwise.
     *
     * @throws InterruptedException
     *         if the waiting thread is interrupted.
     */
    @Override
    public boolean awaitTermination(
            long time,
            TimeUnit unit
    ) throws InterruptedException {

        Objects.requireNonNull(
                unit,
                "Time unit cannot be null"
        );

        log.debug(() ->
                "[AdmissionExecutor][awaitTermination] "
                        + "Waiting for executor termination"
                        + " | timeout=" + time
                        + " | timeUnit=" + unit
                        + " | admittedTasks="
                        + (limit - permits.availablePermits())
        );

        boolean terminated =
                delegate.awaitTermination(time, unit);

        if (terminated) {

            log.info(() ->
                    "[AdmissionExecutor][awaitTermination] "
                            + "Executor terminated successfully"
                            + " | remainingAdmittedTasks="
                            + (limit - permits.availablePermits())
            );

        } else {

            log.warn(
                    "[AdmissionExecutor][awaitTermination] "
                            + "Executor termination wait timed out"
                            + " | remainingAdmittedTasks="
                            + (limit - permits.availablePermits())
                            + " | timeout=" + time
                            + " | timeUnit=" + unit
            );
        }

        return terminated;
    }
}
