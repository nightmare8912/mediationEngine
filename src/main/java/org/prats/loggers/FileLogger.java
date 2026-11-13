
package org.prats.loggers;

import java.io.BufferedWriter;
import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


public final class FileLogger {


    private static final String PROCESS_NAME =
            resolveProcessName();


    private static final boolean IS_LOGGING_ENABLED =
            Boolean.parseBoolean(
                    System.getProperty(
                            "mediation.file-logging.enabled",
                            "true"
                    )
            );

    private static final Path LOG_DIRECTORY =
            Path.of(
                    System.getProperty(
                            "mediation.log.directory",
                            "logs"
                    )
            ).toAbsolutePath().normalize();


    private static final Path LOG_FILE =
            LOG_DIRECTORY.resolve(PROCESS_NAME + ".log");



    private static final int QUEUE_CAPACITY = 16_384;

    private static final BlockingQueue<String> LOG_QUEUE =
            new ArrayBlockingQueue<>(QUEUE_CAPACITY);



    private static final int BATCH_SIZE = 256;

    private static final long BATCH_WINDOW_MILLIS = 200;



    private static final long MAX_FILE_SIZE =
            50L * 1024 * 1024;

    private static final int MAX_BACKUP_FILES = 5;


    private static final AtomicBoolean ACCEPTING_LOGS =
            new AtomicBoolean(true);


    private static final Object ADMISSION_LOCK = new Object();



    private static final AtomicLong DROPPED_MESSAGES =
            new AtomicLong();


    private static final AtomicBoolean IO_FAILURE_REPORTED =
            new AtomicBoolean();



    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern(
                    "yyyy-MM-dd HH:mm:ss.SSS"
            ).withZone(ZoneId.systemDefault());



    private static BufferedWriter writer;

    private static long currentFileSize;



    private static final Thread WRITER_THREAD;


    static {

        if (IS_LOGGING_ENABLED) {

            WRITER_THREAD = new Thread(
                    FileLogger::runWriter,
                    "file-logger-writer"
            );

            WRITER_THREAD.setDaemon(true);

            WRITER_THREAD.start();

            try {

                Runtime.getRuntime().addShutdownHook(
                        new Thread(
                                FileLogger::shutdownAndDrain,
                                "file-logger-shutdown"
                        )
                );

            } catch (IllegalStateException | SecurityException exception) {

                System.err.println(
                        "[FileLogger] Unable to register shutdown hook: "
                                + exception
                );
            }

        } else {

            WRITER_THREAD = null;
        }
    }


    private FileLogger() {

        throw new UnsupportedOperationException(
                "FileLogger is a static utility class"
        );
    }



    private static String resolveProcessName() {

        String name = System.getProperty(
                "mediation.process.name",
                "mediation-engine"
        );

        name = name.trim().replaceAll(
                "[^a-zA-Z0-9._-]",
                "_"
        );

        if (name.isBlank()
                || name.equals(".")
                || name.equals("..")) {

            return "mediation-engine";
        }

        return name;
    }


    public static void log(String message) {

        if (!IS_LOGGING_ENABLED) {
            return;
        }

        String logEntry = buildLogEntry(message);

        synchronized (ADMISSION_LOCK) {

            if (!ACCEPTING_LOGS.get()) {
                return;
            }

            boolean accepted = LOG_QUEUE.offer(logEntry);

            if (!accepted) {
                DROPPED_MESSAGES.incrementAndGet();
            }
        }
    }


    private static String buildLogEntry(String message) {

        return TIMESTAMP_FORMAT.format(Instant.now())
                + " ["
                + Thread.currentThread().getName()
                + "] "
                + String.valueOf(message);
    }


    private static void runWriter() {

        List<String> batch =
                new ArrayList<>(BATCH_SIZE);

        long reportedDroppedMessages = 0;

        try {

            while (
                    ACCEPTING_LOGS.get()
                            || !LOG_QUEUE.isEmpty()
            ) {

                batch.clear();

                String firstMessage;

                try {

                    firstMessage = LOG_QUEUE.poll(
                            BATCH_WINDOW_MILLIS,
                            TimeUnit.MILLISECONDS
                    );

                } catch (InterruptedException exception) {

                    continue;
                }

                if (firstMessage == null) {
                    continue;
                }

                batch.add(firstMessage);

                long deadline =
                        System.nanoTime()
                                + TimeUnit.MILLISECONDS.toNanos(
                                BATCH_WINDOW_MILLIS
                        );

                while (
                        batch.size() < BATCH_SIZE
                                && ACCEPTING_LOGS.get()
                ) {

                    long remaining =
                            deadline - System.nanoTime();

                    if (remaining <= 0) {
                        break;
                    }

                    try {

                        String nextMessage = LOG_QUEUE.poll(
                                remaining,
                                TimeUnit.NANOSECONDS
                        );

                        if (nextMessage == null) {
                            break;
                        }

                        batch.add(nextMessage);

                    } catch (InterruptedException exception) {

                        break;
                    }
                }

                if (!ACCEPTING_LOGS.get()) {

                    LOG_QUEUE.drainTo(
                            batch,
                            BATCH_SIZE - batch.size()
                    );
                }

                reportedDroppedMessages = writeBatch(
                        batch,
                        reportedDroppedMessages
                );
            }

        } finally {

            closeWriterQuietly();
        }
    }


    private static long writeBatch(
            List<String> batch,
            long reportedDroppedMessages
    ) {

        try {

            long totalDropped = DROPPED_MESSAGES.get();

            if (totalDropped > reportedDroppedMessages) {

                long newDrops =
                        totalDropped - reportedDroppedMessages;

                writeLine(
                        buildLogEntry(
                                "[FileLogger] WARNING: "
                                        + newDrops
                                        + " log messages were dropped."
                        )
                );

                reportedDroppedMessages = totalDropped;
            }

            for (String message : batch) {

                writeLine(message);
            }

            if (writer != null) {

                writer.flush();
            }

            IO_FAILURE_REPORTED.set(false);

        } catch (IOException exception) {


            DROPPED_MESSAGES.addAndGet(batch.size());

            closeWriterQuietly();

            if (IO_FAILURE_REPORTED.compareAndSet(false, true)) {

                System.err.println(
                        "[FileLogger] File write failed: "
                                + exception
                );
            }
        }

        return reportedDroppedMessages;
    }


    private static void writeLine(String message)
            throws IOException {

        initializeWriter();

        long entrySize =
                message.getBytes(StandardCharsets.UTF_8).length
                        + System.lineSeparator()
                        .getBytes(StandardCharsets.UTF_8).length;

        if (
                currentFileSize > 0
                        && currentFileSize + entrySize > MAX_FILE_SIZE
        ) {

            rotateLogs();

            initializeWriter();
        }

        writer.write(message);

        writer.newLine();

        currentFileSize += entrySize;
    }


    private static void initializeWriter()
            throws IOException {

        if (writer != null) {
            return;
        }

        Files.createDirectories(LOG_DIRECTORY);

        writer = Files.newBufferedWriter(
                LOG_FILE,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.WRITE
        );

        currentFileSize = Files.size(LOG_FILE);
    }


    private static void rotateLogs()
            throws IOException {

        closeWriter();

        Files.createDirectories(LOG_DIRECTORY);

        Path oldestBackup = LOG_DIRECTORY.resolve(
                PROCESS_NAME + ".log." + MAX_BACKUP_FILES
        );

        Files.deleteIfExists(oldestBackup);

        for (
                int i = MAX_BACKUP_FILES - 1;
                i >= 1;
                i--
        ) {

            Path source = LOG_DIRECTORY.resolve(
                    PROCESS_NAME + ".log." + i
            );

            Path target = LOG_DIRECTORY.resolve(
                    PROCESS_NAME + ".log." + (i + 1)
            );

            if (Files.exists(source)) {

                Files.move(
                        source,
                        target,
                        StandardCopyOption.REPLACE_EXISTING
                );
            }
        }

        if (Files.exists(LOG_FILE)) {

            Files.move(
                    LOG_FILE,
                    LOG_DIRECTORY.resolve(
                            PROCESS_NAME + ".log.1"
                    ),
                    StandardCopyOption.REPLACE_EXISTING
            );
        }

        currentFileSize = 0;
    }


    private static void closeWriter()
            throws IOException {

        BufferedWriter current = writer;

        writer = null;

        if (current != null) {

            current.close();
        }
    }


    private static void closeWriterQuietly() {

        try {

            if (writer != null) {

                writer.flush();

                closeWriter();
            }

        } catch (IOException exception) {

            System.err.println(
                    "[FileLogger] Failed to close log file: "
                            + exception
            );
        }
    }



    private static void shutdownAndDrain() {

        synchronized (ADMISSION_LOCK) {

            ACCEPTING_LOGS.set(false);
        }

        if (WRITER_THREAD == null) {
            return;
        }

        try {


            WRITER_THREAD.join(
                    TimeUnit.SECONDS.toMillis(30)
            );

            if (WRITER_THREAD.isAlive()) {

                System.err.println(
                        "[FileLogger] Writer did not finish within "
                                + "30 seconds. Remaining messages: "
                                + LOG_QUEUE.size()
                );
            }

        } catch (InterruptedException exception) {

            Thread.currentThread().interrupt();

            System.err.println(
                    "[FileLogger] Interrupted while draining log queue."
            );
        }
    }
}
