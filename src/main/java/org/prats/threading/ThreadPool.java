
package org.prats.threading;

import org.prats.loggers.EngineLogger;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPool {

    private static final EngineLogger log =
            EngineLogger.getLogger(ThreadPool.class);

    // =========================================================
    // 1. EXECUTOR CONFIGURATION
    // =========================================================

    /*
     * Default number of admitted tasks.
     *
     * This includes:
     *
     * 1. Tasks currently running.
     * 2. Tasks accepted but waiting to execute.
     *
     * Can be overridden using:
     *
     * -Dmediation.admission.max-tasks=2048
     */
    private static final int DEFAULT_ADMISSION_LIMIT = 1024;

    private static final String ADMISSION_LIMIT_PROPERTY =
            "mediation.admission.max-tasks";

    /*
     * Capacity of the underlying platform-thread
     * executor's work queue.
     *
     * This is separate from the AdmissionExecutor limit.
     *
     * The platform executor can independently reject
     * submissions if its own queue and worker capacity
     * are exhausted.
     */
    private static final int PLATFORM_QUEUE_CAPACITY = 1000;


    // =========================================================
    // 2. INITIALIZE VIRTUAL THREAD EXECUTOR
    // =========================================================

    /**
     * Creates a virtual-thread-per-task executor.
     *
     * Every accepted task is assigned a virtual thread.
     *
     * The executor does not maintain a fixed-size
     * application-level pool of virtual threads.
     *
     * IMPORTANT:
     *
     * This executor does not provide its own
     * admission limit.
     *
     * Admission control is supplied separately
     * by our AdmissionExecutor wrapper.
     *
     * @return Virtual-thread ExecutorService.
     */
    private ExecutorService initializeAndGetVirtualThreadPool() {

        log.debug(() ->
                "[ThreadPool][initializeAndGetVirtualThreadPool] "
                        + "Initializing virtual-thread-per-task executor"
        );

        ExecutorService executor =
                Executors.newVirtualThreadPerTaskExecutor();

        log.info(() ->
                "[ThreadPool][initializeAndGetVirtualThreadPool] "
                        + "Virtual-thread executor initialized successfully"
        );

        return executor;
    }


    // =========================================================
    // 3. INITIALIZE PLATFORM THREAD EXECUTOR
    // =========================================================

    /**
     * Creates a bounded platform-thread executor.
     *
     * This executor is useful when controlling
     * the number of concurrently executing
     * CPU-intensive tasks.
     *
     * Examples:
     *
     * - Password hashing.
     * - Cryptographic verification.
     * - CPU-intensive transformations.
     *
     * ThreadPoolExecutor maintains:
     *
     * 1. Core worker threads.
     * 2. Additional worker threads up to maxThreads.
     * 3. A bounded task queue.
     *
     * @param coreThreads Number of core worker threads.
     * @param maxThreads Maximum number of worker threads.
     * @param idleTimeout Non-core worker idle timeout in ms.
     *
     * @return Platform-thread ExecutorService.
     */
    private ExecutorService initializeAndGetPlatformThreadPool(
            int coreThreads,
            int maxThreads,
            long idleTimeout
    ) {

        // -----------------------------------------------------
        // VALIDATE CONFIGURATION
        // -----------------------------------------------------

        if (coreThreads < 0) {

            log.error(
                    "[ThreadPool][initializeAndGetPlatformThreadPool] "
                            + "Invalid core thread count"
                            + " | coreThreads=" + coreThreads
            );

            throw new IllegalArgumentException(
                    "Core thread count cannot be negative"
            );
        }

        if (maxThreads < 1 || maxThreads < coreThreads) {

            log.error(
                    "[ThreadPool][initializeAndGetPlatformThreadPool] "
                            + "Invalid maximum thread count"
                            + " | coreThreads=" + coreThreads
                            + " | maxThreads=" + maxThreads
            );

            throw new IllegalArgumentException(
                    "Maximum threads must be positive and >= core threads"
            );
        }

        if (idleTimeout < 0) {

            log.error(
                    "[ThreadPool][initializeAndGetPlatformThreadPool] "
                            + "Invalid idle timeout"
                            + " | idleTimeoutMillis=" + idleTimeout
            );

            throw new IllegalArgumentException(
                    "Idle timeout cannot be negative"
            );
        }

        // -----------------------------------------------------
        // INITIALIZE EXECUTOR
        // -----------------------------------------------------

        log.debug(() ->
                "[ThreadPool][initializeAndGetPlatformThreadPool] "
                        + "Creating platform-thread executor"
                        + " | coreThreads=" + coreThreads
                        + " | maxThreads=" + maxThreads
                        + " | idleTimeoutMillis=" + idleTimeout
                        + " | queueCapacity=" + PLATFORM_QUEUE_CAPACITY
        );

        /*
         * ThreadPoolExecutor arguments:
         *
         * coreThreads:
         *     Number of core worker threads.
         *
         * maxThreads:
         *     Maximum number of worker threads.
         *
         * idleTimeout:
         *     Time an excess non-core worker may
         *     remain idle before termination.
         *
         * TimeUnit.MILLISECONDS:
         *     Unit of idleTimeout.
         *
         * ArrayBlockingQueue:
         *     Bounded waiting queue.
         *
         * AbortPolicy:
         *     Rejects excess work by throwing
         *     RejectedExecutionException.
         */
        ExecutorService executor =
                new ThreadPoolExecutor(
                        coreThreads,
                        maxThreads,
                        idleTimeout,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(
                                PLATFORM_QUEUE_CAPACITY
                        ),
                        new ThreadPoolExecutor.AbortPolicy()
                );

        log.info(() ->
                "[ThreadPool][initializeAndGetPlatformThreadPool] "
                        + "Platform-thread executor initialized"
                        + " | coreThreads=" + coreThreads
                        + " | maxThreads=" + maxThreads
                        + " | queueCapacity=" + PLATFORM_QUEUE_CAPACITY
        );

        return executor;
    }


    // =========================================================
    // 4. RESOLVE DEFAULT ADMISSION LIMIT
    // =========================================================

    /**
     * Reads the default admission limit from
     * the JVM system properties.
     *
     * Example JVM argument:
     *
     * -Dmediation.admission.max-tasks=2048
     *
     * If the property is absent or invalid,
     * the configured default is used.
     *
     * @return Positive admission limit.
     */
    private int resolveAdmissionLimit() {

        String configuredValue =
                System.getProperty(
                        ADMISSION_LIMIT_PROPERTY
                );

        if (configuredValue == null || configuredValue.isBlank()) {

            log.debug(() ->
                    "[ThreadPool][resolveAdmissionLimit] "
                            + "Using default admission limit"
                            + " | limit=" + DEFAULT_ADMISSION_LIMIT
            );

            return DEFAULT_ADMISSION_LIMIT;
        }

        try {

            int configuredLimit =
                    Integer.parseInt(
                            configuredValue.trim()
                    );

            if (configuredLimit < 1) {

                log.warn(
                        "[ThreadPool][resolveAdmissionLimit] "
                                + "Configured admission limit must be positive"
                                + " | usingDefault="
                                + DEFAULT_ADMISSION_LIMIT
                );

                return DEFAULT_ADMISSION_LIMIT;
            }

            log.debug(() ->
                    "[ThreadPool][resolveAdmissionLimit] "
                            + "Configured admission limit resolved"
                            + " | limit=" + configuredLimit
            );

            return configuredLimit;

        } catch (NumberFormatException exception) {

            log.warn(
                    "[ThreadPool][resolveAdmissionLimit] "
                            + "Invalid admission limit system property"
                            + " | usingDefault="
                            + DEFAULT_ADMISSION_LIMIT
            );

            return DEFAULT_ADMISSION_LIMIT;
        }
    }


    // =========================================================
    // 5. INITIALIZE THREAD POOL - DEFAULT LIMIT
    // =========================================================

    /**
     * Initializes an executor using the default
     * or JVM-configured admission limit.
     *
     * Preserves the original factory method.
     *
     * @param coreThreads Number of core platform threads.
     * @param maxThreads Maximum platform thread count.
     * @param idleTimeout Idle timeout in milliseconds.
     * @param createPlatformThread True for platform threads.
     *
     * @return Admission-controlled ExecutorService.
     */
    public ExecutorService initializeAndGetThreadPool(
            int coreThreads,
            int maxThreads,
            long idleTimeout,
            boolean createPlatformThread
    ) {

        return initializeAndGetThreadPool(
                coreThreads,
                maxThreads,
                idleTimeout,
                createPlatformThread,
                resolveAdmissionLimit()
        );
    }


    // =========================================================
    // 6. INITIALIZE THREAD POOL - CUSTOM LIMIT
    // =========================================================

    /**
     * Initializes an executor with an explicitly
     * configured admission limit.
     *
     * Useful for services with different
     * workload characteristics.
     *
     * Example:
     *
     * AuthenticationService:
     *     Small concurrency and admission limits.
     *
     * MessageService:
     *     Higher admission capacity.
     *
     * IMPORTANT:
     *
     * For virtual threads, coreThreads, maxThreads
     * and idleTimeout are not used.
     *
     * The AdmissionExecutor still limits the total
     * number of admitted virtual-thread tasks.
     *
     * @param coreThreads Number of core platform threads.
     * @param maxThreads Maximum platform thread count.
     * @param idleTimeout Idle timeout in milliseconds.
     * @param createPlatformThread True for platform threads.
     * @param admissionLimit Maximum admitted tasks.
     *
     * @return Admission-controlled ExecutorService.
     */
    public ExecutorService initializeAndGetThreadPool(
            int coreThreads,
            int maxThreads,
            long idleTimeout,
            boolean createPlatformThread,
            int admissionLimit
    ) {

        // -----------------------------------------------------
        // VALIDATE ADMISSION LIMIT
        // -----------------------------------------------------

        if (admissionLimit < 1) {

            log.error(
                    "[ThreadPool][initializeAndGetThreadPool] "
                            + "Invalid admission limit"
                            + " | admissionLimit=" + admissionLimit
            );

            throw new IllegalArgumentException(
                    "Admission limit must be positive"
            );
        }

        // -----------------------------------------------------
        // DETERMINE EXECUTOR TYPE
        // -----------------------------------------------------

        log.debug(() ->
                "[ThreadPool][initializeAndGetThreadPool] "
                        + "Initializing executor"
                        + " | executorType="
                        + (createPlatformThread ? "PLATFORM" : "VIRTUAL")
                        + " | admissionLimit=" + admissionLimit
        );

        ExecutorService delegate;

        if (createPlatformThread) {

            /*
             * Platform-thread pool.
             *
             * Core count, maximum count and idle
             * timeout determine worker behavior.
             */
            delegate = initializeAndGetPlatformThreadPool(
                    coreThreads,
                    maxThreads,
                    idleTimeout
            );

            /*
             * The platform-thread executor has
             * its own bounded queue.
             *
             * That queue can reject tasks before
             * the admission limit is exhausted.
             */
            if ((long) admissionLimit >
                    (long) maxThreads + PLATFORM_QUEUE_CAPACITY) {

                log.warn(
                        "[ThreadPool][initializeAndGetThreadPool] "
                                + "Admission limit exceeds platform executor's maximum task capacity"
                                + " | admissionLimit=" + admissionLimit
                                + " | maxThreads=" + maxThreads
                                + " | queueCapacity=" + PLATFORM_QUEUE_CAPACITY
                );
            }

        } else {

            /*
             * Virtual-thread executor.
             *
             * Worker pool sizing arguments are
             * intentionally ignored.
             *
             * AdmissionExecutor provides the
             * application-level concurrency bound.
             */
            delegate = initializeAndGetVirtualThreadPool();

            log.debug(() ->
                    "[ThreadPool][initializeAndGetThreadPool] "
                            + "Virtual-thread executor selected"
                            + " | platformThreadParametersIgnored=true"
            );
        }

        // -----------------------------------------------------
        // APPLY ADMISSION CONTROL
        // -----------------------------------------------------

        /*
         * Wrap the delegate executor.
         *
         * AdmissionExecutor uses a Semaphore to
         * bound the number of running plus queued tasks.
         *
         * Its execute() method uses tryAcquire(),
         * so submissions never wait for capacity.
         */
        try {

            AdmissionExecutor boundedExecutor =
                    new AdmissionExecutor(
                            delegate,
                            admissionLimit
                    );

            log.info(() ->
                    "[ThreadPool][initializeAndGetThreadPool] "
                            + "Admission-controlled executor initialized"
                            + " | executorType="
                            + (createPlatformThread ? "PLATFORM" : "VIRTUAL")
                            + " | admissionLimit=" + admissionLimit
            );

            return boundedExecutor;

        } catch (RuntimeException exception) {

            /*
             * If wrapper initialization unexpectedly
             * fails, shut down the delegate to avoid
             * leaking executor resources.
             */
            delegate.shutdownNow();

            log.error(
                    "[ThreadPool][initializeAndGetThreadPool] "
                            + "Failed to initialize admission-controlled executor",
                    exception
            );

            throw exception;
        }
    }


    // =========================================================
    // 7. ADMISSION METRICS
    // =========================================================

    /**
     * Retrieves admission metrics from an executor
     * created by this factory.
     *
     * If the executor is not an AdmissionExecutor,
     * an empty map is returned.
     *
     * Example returned values:
     *
     * admissionLimit      -> 1024
     * admittedTasks       -> 120
     * admissionRejections -> 5
     *
     * admittedTasks represents currently occupied
     * permits, not total lifetime submissions.
     *
     * @param executor Executor to inspect.
     *
     * @return Admission metrics or empty map.
     */
    public static Map<String, Object> admissionMetrics(
            ExecutorService executor
    ) {

        if (executor instanceof AdmissionExecutor bounded) {

            return bounded.metrics();
        }

        return Map.of();
    }
}
