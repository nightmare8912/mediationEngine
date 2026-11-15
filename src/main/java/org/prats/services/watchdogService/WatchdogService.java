
package org.prats.services.watchdogService;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.prats.container.launchers.ContainerLauncher;
import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.ExternalChanelCommunicator;
import org.prats.definitions.interfaces.Service;

import org.prats.definitions.interfaces.TCPMessageHandler;
import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;
import org.prats.threading.ThreadPool;

import java.io.IOException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * WATCHDOG service responsible for managing execution container
 * processes on the local machine.
 *
 * The WATCHDOG acts as the PLATFORM's local process supervisor.
 * It receives control requests from the PLATFORM and performs
 * process lifecycle operations.
 *
 * Supported operations:
 * - START
 * - STOP
 * - RESTART
 *
 * Each execution container is tracked using its Process object.
 *
 * The WATCHDOG does not automatically discover processes that
 * were launched by a previous WATCHDOG JVM instance.
 *
 * Shutting down the WATCHDOG does not terminate its managed ECs.
 */
public class WatchdogService
        implements Service, TCPMessageHandler {

    private static final EngineLogger log =
            EngineLogger.getLogger(WatchdogService.class);

    private final MessageParser messageParser;

    private final Map<String, String> ecConfigPaths;

    private final Map<String, Process> processes;

    private ExecutorService watchdogWorkers;

    private Service service;

    private ExternalChanelCommunicator externalChanelCommunicator;


    // =========================================================
    // 1. CONSTRUCTOR
    // =========================================================

    public WatchdogService(
            MessageParser messageParser,
            Map<String, String> ecConfigPaths
    ) {

        this.messageParser = Objects.requireNonNull(
                messageParser,
                "Message parser cannot be null"
        );

        this.ecConfigPaths = Objects.requireNonNull(
                ecConfigPaths,
                "EC configuration paths cannot be null"
        );

        this.processes = new ConcurrentHashMap<>();

        log.info(() ->
                "[WatchdogService][constructor] WATCHDOG initialized"
                        + " | configuredECs=" + ecConfigPaths.size()
                        + " | trackedProcesses=" + processes.size()
        );
    }


    // =========================================================
    // 2. DEPENDENCY INJECTION
    // =========================================================

    public void setService(Service service) {

        this.service = service;

        log.debug(() ->
                "[WatchdogService][setService] Transport service configured"
                        + " | serviceType="
                        + (service == null
                        ? "null"
                        : service.getClass().getSimpleName())
        );
    }


    public void setControlChannelCommunicator(
            ExternalChanelCommunicator externalChanelCommunicator
    ) {

        this.externalChanelCommunicator = Objects.requireNonNull(
                externalChanelCommunicator,
                "ControlChannelCommunicator cannot be null"
        );

        log.debug(() ->
                "[WatchdogService][setControlChannelCommunicator] "
                        + "Control communicator configured"
                        + " | communicatorType="
                        + externalChanelCommunicator
                        .getClass()
                        .getSimpleName()
        );
    }


    // =========================================================
    // 3. EXTRACT EC NAME FROM REQUEST
    // =========================================================

    public String getProcessFromRequest(Message request) {

        String processName;

        try {

            processName = messageParser.fromPayload(request.getPayload(), org.prats.definitions.records.ControlRequest.class).ecName();

            if (processName != null && processName.isBlank()) {
                processName = null;
            }

        } catch (JsonProcessingException exception) {

            log.warn(() ->
                    "[WatchdogService][getProcessFromRequest] "
                            + "Unable to extract EC name"
                            + " | reason=" + exception.getMessage()
            );

            processName = null;
        }

        final String extractedName = processName;

        log.debug(() ->
                "[WatchdogService][getProcessFromRequest] "
                        + "EC name extraction completed"
                        + " | ecName=" + extractedName
        );

        return processName;
    }


    // =========================================================
    // 4. PROCESS REGISTRY
    // =========================================================

    /**
     * Retrieves a process launched and registered by this
     * WATCHDOG instance.
     *
     * No operating-system process discovery is performed.
     */
    private Process getProcess(String processName) {

        Process process = processes.get(processName);

        log.debug(() ->
                "[WatchdogService][getProcess] Process lookup completed"
                        + " | ecName=" + processName
                        + " | found=" + (process != null)
                        + " | alive="
                        + (process != null && process.isAlive())
        );

        return process;
    }


    // =========================================================
    // 5. START EXECUTION CONTAINER
    // =========================================================

    private synchronized Message handleStart(Message request) {

        String ecName = getProcessFromRequest(request);

        log.info(() ->
                "[WatchdogService][handleStart] EC start requested"
                        + " | ecName=" + ecName
                        + " | correlationId=" + request.getCorrelationId()
        );


        // -----------------------------------------------------
        // 5.1. Validate process name
        // -----------------------------------------------------

        if (ecName == null) {

            log.warn(
                    "[WatchdogService][handleStart] "
                            + "Start rejected because EC name is invalid"
            );

            return createErrorResponse(
                    request,
                    "UNABLE_TO_START",
                    "Invalid process name"
            );
        }


        // -----------------------------------------------------
        // 5.2. Check existing process
        // -----------------------------------------------------

        Process processCheck = getProcess(ecName);

        if (processCheck != null && processCheck.isAlive()) {

            log.warn(() ->
                    "[WatchdogService][handleStart] "
                            + "EC is already running"
                            + " | ecName=" + ecName
                            + " | pid=" + processCheck.pid()
            );

            return createErrorResponse(
                    request,
                    "UNABLE_TO_START",
                    "The container is already running."
            );
        }


        // -----------------------------------------------------
        // 5.3. Remove stale process registration
        // -----------------------------------------------------

        if (processCheck != null) {

            processes.remove(ecName, processCheck);

            log.debug(() ->
                    "[WatchdogService][handleStart] "
                            + "Removed stale EC process registration"
                            + " | ecName=" + ecName
                            + " | pid=" + processCheck.pid()
            );
        }


        // -----------------------------------------------------
        // 5.4. Resolve EC configuration
        // -----------------------------------------------------

        String ecConfigPath = ecConfigPaths.get(ecName);

        if (ecConfigPath == null || ecConfigPath.isBlank()) {

            log.warn(() ->
                    "[WatchdogService][handleStart] "
                            + "No EC configuration registered"
                            + " | ecName=" + ecName
            );

            return createErrorResponse(
                    request,
                    "UNABLE_TO_START",
                    "No configuration path is registered for EC: " + ecName
            );
        }

        log.debug(() ->
                "[WatchdogService][handleStart] "
                        + "EC configuration resolved"
                        + " | ecName=" + ecName
                        + " | configPath=" + ecConfigPath
        );


        // -----------------------------------------------------
        // 5.5. Launch EC JVM
        // -----------------------------------------------------

        try {

            sendProgressResponse(
                    request,
                    "Configuration validated for EC " + ecName
                            + ". Launching process."
            );

            log.info(() ->
                    "[WatchdogService][handleStart] Launching EC JVM"
                            + " | ecName=" + ecName
            );

            Process process = new ContainerLauncher().launchEC(
                    ecConfigPath,
                    List.of()
            );


            // -------------------------------------------------
            // 5.6. Validate launcher result
            // -------------------------------------------------

            if (process == null) {

                log.error(
                        "[WatchdogService][handleStart] "
                                + "ContainerLauncher returned null"
                                + " | ecName=" + ecName
                );

                return createErrorResponse(
                        request,
                        "UNABLE_TO_START",
                        "Container launcher returned a null process."
                );
            }

            final Process launchedProcess = process;

            log.info(() ->
                    "[WatchdogService][handleStart] "
                            + "EC JVM process created"
                            + " | ecName=" + ecName
                            + " | pid=" + launchedProcess.pid()
            );

            sendProgressResponse(
                    request,
                    "EC " + ecName
                            + " process created. PID: " + process.pid()
                            + ". Verifying that the process is running."
            );


            // -------------------------------------------------
            // 5.7. Verify process liveness
            // -------------------------------------------------

            if (!process.isAlive()) {

                int exitCode = process.exitValue();

                log.error(
                        "[WatchdogService][handleStart] "
                                + "EC process exited immediately"
                                + " | ecName=" + ecName
                                + " | pid=" + process.pid()
                                + " | exitCode=" + exitCode
                );

                return createErrorResponse(
                        request,
                        "UNABLE_TO_START",
                        "The container process exited immediately. "
                                + "Exit code: " + exitCode
                );
            }


            // -------------------------------------------------
            // 5.8. Register process
            // -------------------------------------------------

            sendProgressResponse(
                    request,
                    "EC " + ecName
                            + " process is running. "
                            + "Registering process with WATCHDOG."
            );

            processes.put(ecName, process);

            log.info(() ->
                    "[WatchdogService][handleStart] "
                            + "EC process registered successfully"
                            + " | ecName=" + ecName
                            + " | pid=" + launchedProcess.pid()
                            + " | trackedProcesses=" + processes.size()
            );


            // -------------------------------------------------
            // 5.9. Build successful response
            // -------------------------------------------------

            var response = new org.prats.definitions.records.ControlResponse("Process started. PID: " + process.pid(), "Process started. PID: " + process.pid());

            return createResponseMessage(
                    request,
                    response
            );

        } catch (IOException | SecurityException exception) {

            log.error(
                    "[WatchdogService][handleStart] "
                            + "Unable to start execution container"
                            + " | ecName=" + ecName,
                    exception
            );

            return createErrorResponse(
                    request,
                    "UNABLE_TO_START",
                    "Error occurred while starting process: "
                            + exception.getMessage()
            );
        }
    }


    // =========================================================
    // 6. STOP EXECUTION CONTAINER
    // =========================================================

    private synchronized Message handleStop(Message request) {

        String ecName = getProcessFromRequest(request);

        log.info(() ->
                "[WatchdogService][handleStop] EC stop requested"
                        + " | ecName=" + ecName
                        + " | correlationId=" + request.getCorrelationId()
        );


        // -----------------------------------------------------
        // 6.1. Validate EC name
        // -----------------------------------------------------

        if (ecName == null) {

            log.warn(
                    "[WatchdogService][handleStop] "
                            + "Stop rejected because EC name is invalid"
            );

            return createErrorResponse(
                    request,
                    "UNABLE_TO_KILL",
                    "Invalid process name"
            );
        }


        // -----------------------------------------------------
        // 6.2. Retrieve existing process
        // -----------------------------------------------------

        Process processCheck = getProcess(ecName);

        if (processCheck == null || !processCheck.isAlive()) {

            if (processCheck != null) {
                processes.remove(ecName, processCheck);
            }

            log.info(() ->
                    "[WatchdogService][handleStop] "
                            + "EC is already stopped or not registered"
                            + " | ecName=" + ecName
            );

            var response = new org.prats.definitions.records.ControlResponse("Process is already stopped.", "Process is already stopped.");

            return createResponseMessage(
                    request,
                    response
            );
        }


        // -----------------------------------------------------
        // 6.3. Prepare process termination
        // -----------------------------------------------------

        Process processToStop = processCheck;

        long processId = processToStop.pid();

        log.info(() ->
                "[WatchdogService][handleStop] "
                        + "Beginning EC process termination"
                        + " | ecName=" + ecName
                        + " | pid=" + processId
        );

        try {


            // -------------------------------------------------
            // 6.4. Request graceful process termination
            // -------------------------------------------------

            sendProgressResponse(
                    request,
                    "Sending graceful termination signal to EC "
                            + ecName + ". PID: " + processId
            );

            processToStop.destroy();

            log.debug(() ->
                    "[WatchdogService][handleStop] "
                            + "Graceful termination signal sent"
                            + " | ecName=" + ecName
                            + " | pid=" + processId
            );

            sendProgressResponse(
                    request,
                    "Graceful termination signal sent. "
                            + "Waiting up to 15 seconds for EC "
                            + ecName + " to terminate."
            );


            // -------------------------------------------------
            // 6.5. Wait for graceful termination
            // -------------------------------------------------

            boolean terminated = processToStop.waitFor(
                    15,
                    TimeUnit.SECONDS
            );


            // -------------------------------------------------
            // 6.6. Force termination if necessary
            // -------------------------------------------------

            if (!terminated) {

                log.warn(() ->
                        "[WatchdogService][handleStop] "
                                + "Graceful termination timed out"
                                + " | ecName=" + ecName
                                + " | pid=" + processId
                                + " | action=FORCE_TERMINATE"
                );

                sendProgressResponse(
                        request,
                        "Graceful termination timed out. "
                                + "Sending forced termination signal to EC "
                                + ecName + ". PID: " + processId
                );

                processToStop.destroyForcibly();

                sendProgressResponse(
                        request,
                        "Forced termination signal sent. "
                                + "Waiting up to 5 seconds for EC "
                                + ecName + " to terminate."
                );

                terminated = processToStop.waitFor(
                        5,
                        TimeUnit.SECONDS
                );
            }


            // -------------------------------------------------
            // 6.7. Verify process termination
            // -------------------------------------------------

            if (!terminated) {

                log.error(
                        "[WatchdogService][handleStop] "
                                + "Unable to terminate EC process"
                                + " | ecName=" + ecName
                                + " | pid=" + processId
                );

                return createErrorResponse(
                        request,
                        "UNABLE_TO_KILL",
                        "Process could not be terminated. PID: "
                                + processId
                );
            }


            // -------------------------------------------------
            // 6.8. Remove process registration
            // -------------------------------------------------

            processes.remove(
                    ecName,
                    processToStop
            );

            log.info(() ->
                    "[WatchdogService][handleStop] "
                            + "EC process terminated successfully"
                            + " | ecName=" + ecName
                            + " | pid=" + processId
                            + " | trackedProcesses=" + processes.size()
            );


            // -------------------------------------------------
            // 6.9. Return successful response
            // -------------------------------------------------

            var response = new org.prats.definitions.records.ControlResponse("Process stopped. PID: " + processId, "Process stopped. PID: " + processId);

            return createResponseMessage(
                    request,
                    response
            );

        } catch (InterruptedException exception) {

            Thread.currentThread().interrupt();

            log.warn(() ->
                    "[WatchdogService][handleStop] "
                            + "Interrupted while terminating EC"
                            + " | ecName=" + ecName
                            + " | pid=" + processId
            );

            return createErrorResponse(
                    request,
                    "INTERRUPTED",
                    "Interrupted while waiting for process termination."
            );

        } catch (SecurityException exception) {

            log.error(
                    "[WatchdogService][handleStop] "
                            + "Permission denied while terminating EC"
                            + " | ecName=" + ecName
                            + " | pid=" + processId,
                    exception
            );

            return createErrorResponse(
                    request,
                    "UNABLE_TO_KILL",
                    "Permission denied while terminating process: "
                            + exception.getMessage()
            );
        }
    }


    // =========================================================
    // 7. RESTART EXECUTION CONTAINER
    // =========================================================

    private synchronized Message handleRestart(Message request) {

        String ecName = getProcessFromRequest(request);

        log.info(() ->
                "[WatchdogService][handleRestart] EC restart requested"
                        + " | ecName=" + ecName
                        + " | correlationId=" + request.getCorrelationId()
        );

        if (ecName == null) {

            log.warn(
                    "[WatchdogService][handleRestart] "
                            + "Restart rejected because EC name is invalid"
            );

            return createErrorResponse(
                    request,
                    "UNABLE_TO_START",
                    "Invalid process name"
            );
        }


        // -----------------------------------------------------
        // 7.1. Retrieve existing process
        // -----------------------------------------------------

        Process processCheck = getProcess(ecName);


        // -----------------------------------------------------
        // 7.2. Stop existing EC
        // -----------------------------------------------------

        log.debug(() ->
                "[WatchdogService][handleRestart] "
                        + "Stopping existing EC before restart"
                        + " | ecName=" + ecName
        );

        Message stopResponse = handleStop(request);


        // -----------------------------------------------------
        // 7.3. Verify stop completion
        // -----------------------------------------------------

        if ((processCheck != null && processCheck.isAlive())
                || Thread.currentThread().isInterrupted()) {

            log.warn(() ->
                    "[WatchdogService][handleRestart] "
                            + "Restart aborted"
                            + " | ecName=" + ecName
                            + " | processStillAlive="
                            + (processCheck != null && processCheck.isAlive())
                            + " | interrupted="
                            + Thread.currentThread().isInterrupted()
            );

            return stopResponse;
        }


        // -----------------------------------------------------
        // 7.4. Start EC again
        // -----------------------------------------------------

        log.info(() ->
                "[WatchdogService][handleRestart] "
                        + "Previous EC stopped; starting new process"
                        + " | ecName=" + ecName
        );

        return handleStart(request);
    }


    // =========================================================
    // 8. CREATE SUCCESS RESPONSE
    // =========================================================

    private Message createResponseMessage(
            Message request,
            org.prats.definitions.records.ControlResponse messageValues
    ) {

        Message response = messageParser.createResponse(
                request.getCorrelationId(),
                messageValues
        );

        response.setStatus("SUCCESS");
        response.setResponseComplete(true);

        log.debug(() ->
                "[WatchdogService][createResponseMessage] "
                        + "Success response created"
                        + " | correlationId=" + request.getCorrelationId()
        );

        return response;
    }


    // =========================================================
    // 9. CREATE ERROR RESPONSE
    // =========================================================

    private Message createErrorResponse(
            Message request,
            String errorCode,
            String errorReason
    ) {

        Message response = messageParser.createErrorResponse(
                request.getCorrelationId(),
                errorCode,
                errorReason
        );

        response.setStatus("FAILED");
        response.setResponseComplete(true);

        log.debug(() ->
                "[WatchdogService][createErrorResponse] "
                        + "Error response created"
                        + " | correlationId=" + request.getCorrelationId()
                        + " | errorCode=" + errorCode
        );

        return response;
    }


    // =========================================================
    // 10. SEND CONTROL RESPONSE
    // =========================================================

    private void sendResponse(Message response) {

        try {

            Objects.requireNonNull(
                    response,
                    "Response cannot be null"
            );

            Objects.requireNonNull(
                    externalChanelCommunicator,
                    "ControlChannelCommunicator has not been initialized"
            ).sendResponse(response);

            log.debug(() ->
                    "[WatchdogService][sendResponse] "
                            + "Response sent to control channel"
                            + " | correlationId=" + response.getCorrelationId()
                            + " | status=" + response.getStatus()
            );

        } catch (RuntimeException exception) {

            log.error(
                    "[WatchdogService][sendResponse] "
                            + "Unable to send control response",
                    exception
            );
        }
    }


    // =========================================================
    // 11. SEND PROGRESS RESPONSE
    // =========================================================

    private void sendProgressResponse(
            Message request,
            String status
    ) {

        var responseValues = new org.prats.definitions.records.ControlResponse(status, status);

        Message response = messageParser.createResponse(
                request.getCorrelationId(),
                responseValues
        );

        response.setStatus("IN_PROGRESS");
        response.setResponseComplete(false);

        log.debug(() ->
                "[WatchdogService][sendProgressResponse] "
                        + "Sending operation progress"
                        + " | correlationId=" + request.getCorrelationId()
                        + " | status=" + status
        );

        sendResponse(response);
    }


    // =========================================================
    // 12. CONTROL REQUEST PROCESSING
    // =========================================================

    private void handleRequest(Message request) {

        Objects.requireNonNull(
                request,
                "Request cannot be null"
        );

        log.info(() ->
                "[WatchdogService][handleRequest] "
                        + "Control request received"
                        + " | operation=" + request.getOperation()
                        + " | correlationId=" + request.getCorrelationId()
        );


        // -----------------------------------------------------
        // 12.1. Validate correlation
        // -----------------------------------------------------

        if (request.getCorrelationId() == null) {

            log.warn(
                    "[WatchdogService][handleRequest] "
                            + "Request ignored because correlation ID is missing"
            );

            return;
        }

        Message response;

        try {


            // -------------------------------------------------
            // 12.2. Validate operation
            // -------------------------------------------------

            if (request.getOperation() == null) {

                log.warn(
                        "[WatchdogService][handleRequest] "
                                + "Control request operation is null"
                );

                response = createErrorResponse(
                        request,
                        "INVALID_OPERATION",
                        "The request does not contain a valid operation."
                );

            } else {


                // ---------------------------------------------
                // 12.3. Dispatch operation
                // ---------------------------------------------

                log.debug(() ->
                        "[WatchdogService][handleRequest] "
                                + "Dispatching control operation"
                                + " | operation=" + request.getOperation()
                );

                response = switch (request.getOperation()) {

                    case START -> handleStart(request);

                    case STOP -> handleStop(request);

                    case RESTART -> handleRestart(request);

                    default -> createErrorResponse(
                            request,
                            "UNSUPPORTED_OPERATION",
                            "Unsupported watchdog operation: "
                                    + request.getOperation()
                    );
                };
            }

        } catch (Exception exception) {

            log.error(
                    "[WatchdogService][handleRequest] "
                            + "Unexpected error while processing control request"
                            + " | correlationId=" + request.getCorrelationId()
                            + " | operation=" + request.getOperation(),
                    exception
            );

            response = createErrorResponse(
                    request,
                    "WATCHDOG_COMMAND_FAILED",
                    "An unexpected error occurred while processing "
                            + "the watchdog command: "
                            + exception.getMessage()
            );
        }


        // -----------------------------------------------------
        // 12.4. Return terminal response
        // -----------------------------------------------------

        final String status = response.getStatus();

        log.debug(() ->
                "[WatchdogService][handleRequest] "
                        + "Control request processing completed"
                        + " | correlationId=" + request.getCorrelationId()
                        + " | responseStatus=" + status
        );

        sendResponse(response);
    }


    // =========================================================
    // 13. CONTROL MESSAGE CALLBACKS
    // =========================================================

    @Override
    public void onRequest(Message request) {

        Objects.requireNonNull(
                request,
                "Request cannot be null"
        );

        log.debug(() ->
                "[WatchdogService][onRequest] "
                        + "Submitting control request to worker pool"
                        + " | operation=" + request.getOperation()
                        + " | correlationId=" + request.getCorrelationId()
        );

        try {

            submitToThreadPool(
                    () -> handleRequest(request)
            );

        } catch (RejectedExecutionException | IllegalStateException exception) {

            log.warn(() ->
                    "[WatchdogService][onRequest] "
                            + "Unable to submit control request"
                            + " | operation=" + request.getOperation()
                            + " | correlationId=" + request.getCorrelationId()
                            + " | reason=" + exception.getMessage()
            );

            if (request.getCorrelationId() != null) {

                sendResponse(
                        createErrorResponse(
                                request,
                                "WATCHDOG_UNAVAILABLE",
                                "WATCHDOG cannot process the request "
                                        + "because its worker pool is unavailable."
                        )
                );
            }
        }
    }


    @Override
    public void onInfo(Message request) {

        log.debug(() ->
                "[WatchdogService][onInfo] "
                        + "Informational control message received"
                        + " | correlationId="
                        + (request == null
                        ? null
                        : request.getCorrelationId())
        );
    }


    @Override
    public void onHeartbeat(Message response) {

        log.debug(() ->
                "[WatchdogService][onHeartbeat] "
                        + "Heartbeat received"
                        + " | correlationId="
                        + (response == null
                        ? null
                        : response.getCorrelationId())
        );
    }


    @Override
    public void onResponse(Message response) {

        log.debug(() ->
                "[WatchdogService][onResponse] "
                        + "Control response received"
                        + " | correlationId="
                        + (response == null
                        ? null
                        : response.getCorrelationId())
        );
    }


    @Override
    public void onTimeout(Message originalRequest, Throwable cause) {

        log.warn(() ->
                "[WatchdogService][onTimeout] "
                        + "Control request timed out"
                        + " | operation="
                        + (originalRequest == null
                        ? null
                        : originalRequest.getOperation())
                        + " | correlationId="
                        + (originalRequest == null
                        ? null
                        : originalRequest.getCorrelationId())
        );
    }


    // =========================================================
    // 14. START WATCHDOG
    // =========================================================

    @Override
    public void start() {

        log.info(
                "[WatchdogService][start] "
                        + "Starting WATCHDOG worker pool"
        );


        // -----------------------------------------------------
        // 14.1. Initialize worker pool
        // -----------------------------------------------------

        watchdogWorkers = new ThreadPool()
                .initializeAndGetThreadPool(
                        1,
                        1,
                        0,
                        true
                );

        log.debug(() ->
                "[WatchdogService][start] "
                        + "WATCHDOG worker pool initialized"
                        + " | executorType="
                        + watchdogWorkers.getClass().getSimpleName()
        );


        // -----------------------------------------------------
        // 14.2. Start transport service
        // -----------------------------------------------------

        try {

            Objects.requireNonNull(
                    service,
                    "Watchdog transport service has not been configured"
            ).start();

            log.info(
                    "[WatchdogService][start] "
                            + "WATCHDOG transport service started successfully"
            );

        } catch (RuntimeException exception) {

            log.error(
                    "[WatchdogService][start] "
                            + "WATCHDOG transport startup failed",
                    exception
            );

            watchdogWorkers.shutdownNow();
            watchdogWorkers = null;

            throw exception;
        }
    }


    // =========================================================
    // 15. SHUTDOWN WATCHDOG
    // =========================================================

    @Override
    public void shutdown() {

        log.info(() ->
                "[WatchdogService][shutdown] "
                        + "WATCHDOG shutdown requested"
                        + " | trackedECProcesses=" + processes.size()
        );

        /*
         * IMPORTANT:
         *
         * Stopping the WATCHDOG must not automatically
         * terminate the execution containers it launched.
         */

        log.info(
                "[WatchdogService][shutdown] "
                        + "Managed EC processes will remain running"
        );


        // -----------------------------------------------------
        // 15.1. Shutdown transport service
        // -----------------------------------------------------

        if (service != null) {

            log.debug(
                    "[WatchdogService][shutdown] "
                            + "Stopping control transport service"
            );

            service.shutdown();
        }


        // -----------------------------------------------------
        // 15.2. Shutdown worker pool
        // -----------------------------------------------------

        ExecutorService workers = watchdogWorkers;

        if (workers != null) {

            workers.shutdown();

            log.debug(
                    "[WatchdogService][shutdown] "
                            + "Worker pool shutdown requested"
            );

            try {

                if (!workers.awaitTermination(
                        5,
                        TimeUnit.SECONDS
                )) {

                    log.warn(
                            "[WatchdogService][shutdown] "
                                    + "Worker pool shutdown timed out"
                                    + " | action=SHUTDOWN_NOW"
                    );

                    workers.shutdownNow();

                } else {

                    log.debug(
                            "[WatchdogService][shutdown] "
                                    + "Worker pool terminated gracefully"
                    );
                }

            } catch (InterruptedException exception) {

                Thread.currentThread().interrupt();

                log.warn(
                        "[WatchdogService][shutdown] "
                                + "Interrupted while awaiting worker termination"
                );

                workers.shutdownNow();
            }

            if (workers.isTerminated()) {
                watchdogWorkers = null;
            }
        }

        log.info(
                "[WatchdogService][shutdown] "
                        + "WATCHDOG shutdown sequence completed"
        );
    }


    // =========================================================
    // 16. QUIESCE WATCHDOG
    // =========================================================

    @Override
    public void quiesce() {

        log.info(
                "[WatchdogService][quiesce] "
                        + "Quiescing WATCHDOG transport"
        );

        if (service != null) {
            service.quiesce();
        }
    }


    // =========================================================
    // 17. DRAIN WATCHDOG
    // =========================================================

    @Override
    public void drain(long timeout) {

        log.info(() ->
                "[WatchdogService][drain] "
                        + "Draining WATCHDOG transport"
                        + " | timeoutMillis=" + timeout
        );

        if (service != null) {
            service.drain(timeout);
        }
    }


    // =========================================================
    // 18. CLEANUP STALE PROCESSES
    // =========================================================

    @Override
    public void cleanup() {

        int before = processes.size();

        processes.entrySet().removeIf(
                entry -> !entry.getValue().isAlive()
        );

        int after = processes.size();

        log.debug(() ->
                "[WatchdogService][cleanup] "
                        + "Stale process cleanup completed"
                        + " | before=" + before
                        + " | after=" + after
                        + " | removed=" + (before - after)
        );
    }


    // =========================================================
    // 19. WORKER POOL SUBMISSION
    // =========================================================

    private void submitToThreadPool(Runnable runnable) {

        Objects.requireNonNull(
                runnable,
                "Runnable cannot be null"
        );

        ExecutorService workers = Objects.requireNonNull(
                watchdogWorkers,
                "WATCHDOG worker pool has not been initialized"
        );

        if (workers.isShutdown()) {

            log.warn(
                    "[WatchdogService][submitToThreadPool] "
                            + "Task submission rejected because worker pool is shut down"
            );

            throw new RejectedExecutionException(
                    "WATCHDOG worker pool is shut down"
            );
        }

        workers.submit(runnable);

        log.debug(() ->
                "[WatchdogService][submitToThreadPool] "
                        + "Task submitted to WATCHDOG worker pool"
                        + " | shutdown=" + workers.isShutdown()
        );
    }
}
