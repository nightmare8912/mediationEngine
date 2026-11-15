package org.prats.loggers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Lightweight logging facade for the mediation engine.
 *
 * Asynchronous logging is handled by Logback's AsyncAppender.
 *
 * Application threads never perform the actual appender I/O.
 */
public final class EngineLogger {

    // =====================================================
    // Logging Configuration
    // =====================================================

    private static final boolean DETAIL_ENABLED =
            Boolean.parseBoolean(
                    System.getProperty(
                            "mediation.logging.detail-enabled",
                            "true"
                    )
            );

    private static final long DEBUG_SAMPLE_WINDOW_NANOS = Math.max(0,
            Long.getLong("mediation.logging.debug-sample-window-ms", 1000L))
            * 1_000_000L;

    private static final ConcurrentHashMap<String, AtomicLong> LAST_DEBUG_BY_SITE =
            new ConcurrentHashMap<>();

    private static final boolean WARN_ENABLED =
            Boolean.parseBoolean(
                    System.getProperty(
                            "mediation.logging.warn-enabled",
                            "true"
                    )
            );

    private static final boolean ERROR_ENABLED =
            Boolean.parseBoolean(
                    System.getProperty(
                            "mediation.logging.error-enabled",
                            "true"
                    )
            );


    // =====================================================
    // Logger Instance
    // =====================================================

    private final Logger logger;


    private EngineLogger(Class<?> owner) {

        this.logger = LoggerFactory.getLogger(
                Objects.requireNonNull(
                        owner,
                        "Logger owner cannot be null"
                )
        );

    }


    // =====================================================
    // Factory
    // =====================================================

    public static EngineLogger getLogger(Class<?> owner) {

        return new EngineLogger(owner);

    }


    // =====================================================
    // DEBUG
    // =====================================================

    public void debug(String message) {

        if (DETAIL_ENABLED && logger.isDebugEnabled()
                && shouldEmitDebug(message)) {
            logger.debug(message);

        }

    }


    public void debug(Supplier<String> messageSupplier) {

        Objects.requireNonNull(
                messageSupplier,
                "Message supplier cannot be null"
        );

        if (DETAIL_ENABLED && logger.isDebugEnabled()) {
            String message = messageSupplier.get();
            if (shouldEmitDebug(message)) {
                logger.debug(message);
            }

        }

    }


    // =====================================================
    // INFO
    // =====================================================

    public void info(String message) {

        if (DETAIL_ENABLED && logger.isInfoEnabled()) {

            logger.info(message);

        }

    }


    public void info(Supplier<String> messageSupplier) {

        Objects.requireNonNull(
                messageSupplier,
                "Message supplier cannot be null"
        );

        if (DETAIL_ENABLED && logger.isInfoEnabled()) {

            logger.info(messageSupplier.get());

        }

    }


    // =====================================================
    // WARN
    // =====================================================

    public void warn(String message) {

        if (WARN_ENABLED && logger.isWarnEnabled()) {

            logger.warn(message);

        }

    }


    public void warn(Supplier<String> messageSupplier) {

        Objects.requireNonNull(
                messageSupplier,
                "Message supplier cannot be null"
        );

        if (WARN_ENABLED && logger.isWarnEnabled()) {

            logger.warn(messageSupplier.get());

        }

    }


    // =====================================================
    // ERROR
    // =====================================================

    public void error(String message) {

        if (ERROR_ENABLED && logger.isErrorEnabled()) {

            logger.error(message);

        }

    }


    public void error(String message, Throwable throwable) {

        if (ERROR_ENABLED && logger.isErrorEnabled()) {

            logger.error(message, throwable);

        }

    }


    // =====================================================
    // Configuration Accessors
    // =====================================================

    public static boolean isDetailEnabled() {

        return DETAIL_ENABLED;

    }

    private boolean shouldEmitDebug(String message) {
        if (DEBUG_SAMPLE_WINDOW_NANOS == 0) {
            return true;
        }

        String site = message == null ? "null" : message;
        int detailsStart = site.indexOf(" | ");
        if (detailsStart >= 0) {
            site = site.substring(0, detailsStart);
        } else if (site.length() > 160) {
            site = site.substring(0, 160);
        }

        AtomicLong lastEmitted = LAST_DEBUG_BY_SITE.computeIfAbsent(
                logger.getName() + ':' + site,
                ignored -> new AtomicLong()
        );

        long now = System.nanoTime();
        long previous = lastEmitted.get();
        while (now - previous >= DEBUG_SAMPLE_WINDOW_NANOS) {
            if (lastEmitted.compareAndSet(previous, now)) {
                return true;
            }
            previous = lastEmitted.get();
        }

        return false;
    }


    public static boolean isWarnEnabled() {

        return WARN_ENABLED;

    }


    public static boolean isErrorEnabled() {

        return ERROR_ENABLED;

    }

}
