package org.prats.services.watchdogService;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.prats.container.launchers.ContainerLauncher;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.ContainerType;
import org.prats.definitions.interfaces.ControlMessageHandler;
import org.prats.definitions.interfaces.Service;
import org.prats.loggers.EngineLogger;
import org.prats.parsers.MessageParser;
import org.prats.definitions.interfaces.ControlChannelCommunicator;
import org.prats.threading.ThreadPool;
import org.prats.utilities.ApplicationPaths;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

// the watchdog service is a lightweight jvm that has only one service (tcp service) keeping it alive
// this jvm owns the actual container running on its server, it's like the platform proxy to control the
// container on a different machine, since the platform cannot kill and re-launch a process on a remote machine.
// the platform can send a kill command to the watchdog if it believes that the ec is unresponsive
// it can also order a kill command if the ec itself escalates to the platform about its unhealthy state.


/*
 * Operation: START
 * payload: {
 *   "ecName": "ec01"
 * }
 * */
public class WatchdogService implements Service, ControlMessageHandler {

    private static final EngineLogger log =
            EngineLogger.getLogger(WatchdogService.class);

    private final MessageParser messageParser;
    private final Map<String, String> ecConfigPaths;;
    private ExecutorService watchdogWorkers;
    private Service service;

    private ControlChannelCommunicator controlChannelCommunicator;

    private final Map<String, ProcessHandle> processes;
    private org.prats.handlers.JVMDiagnosticsHandler diagnostics;
    public void setDiagnostics(org.prats.handlers.JVMDiagnosticsHandler diagnostics) { this.diagnostics = diagnostics; }

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
                "JVM launcher cannot be null"
        );

        this.processes = new ConcurrentHashMap<>();

        log.info(() ->
                "[WatchdogService][constructor] WATCHDOG service initialized"
                        + " | registeredECConfigs=" + this.ecConfigPaths.size()
                        + " | trackedProcesses=" + this.processes.size()
        );
    }

    public void setService(Service service) {
        this.service = service;

        log.debug(() ->
                "[WatchdogService][setService] Transport service configured"
                        + " | serviceType="
                        + (service == null ? "null" : service.getClass().getSimpleName())
        );
    }

    public String getProcessFromRequest(Message request) {
        String processName;
        try {

            processName = messageParser.extractPayloadField(
                    request,
                    "ecName",
                    String.class
            );

            if (processName != null && processName.isBlank()) {
                processName = null;
            }

        } catch (JsonProcessingException jsonProcessingException) {
            log.warn(() ->
                    "[WatchdogService][getProcessFromRequest] Unable to extract EC name"
                            + " | reason=" + jsonProcessingException.getMessage()
            );
            processName = null;
        }

        final String extractedProcessName = processName;
        log.debug(() ->
                "[WatchdogService][getProcessFromRequest] EC name extracted"
                        + " | ecName=" + extractedProcessName
        );

        return processName;
    }

    public void setControlChannelCommunicator(
            ControlChannelCommunicator controlChannelCommunicator
    ) {

        Objects.requireNonNull(
                controlChannelCommunicator,
                "ControlChannelCommunicator cannot be null."
        );

        this.controlChannelCommunicator = controlChannelCommunicator;

        log.debug(() ->
                "[WatchdogService][setControlChannelCommunicator] Control channel communicator configured"
                        + " | communicatorType="
                        + controlChannelCommunicator.getClass().getSimpleName()
        );
    }

    private ProcessHandle getProcess(String processName) {
        ProcessHandle tracked = processes.get(processName);
        if (tracked != null && !tracked.isAlive()) {
            processes.remove(processName, tracked);
            tracked = null;
        }
        if (tracked == null && ecConfigPaths.containsKey(processName)) {
            tracked = ECProcessDiscovery.find(processName, ApplicationPaths.getHome()).orElse(null);
            if (tracked != null) {
                processes.put(processName, tracked);
                log.info("[WatchdogService][getProcess] Reattached to surviving EC"
                        + " | ecName=" + processName + " | pid=" + tracked.pid());
            }
        }
        final ProcessHandle process = tracked;

        log.debug(() ->
                "[WatchdogService][getProcess] Process lookup completed"
                        + " | ecName=" + processName
                        + " | processFound=" + (process != null)
                        + " | processAlive=" + (process != null && process.isAlive())
        );

        return process;
    }

    private synchronized Message handleStart(Message request) {

        log.info(() ->
                "[WatchdogService][handleStart] Start request received"
                        + " | correlationId=" + request.getCorrelationId()
        );

        String ecName = getProcessFromRequest(request);

        if (ecName == null) {

            log.warn(() ->
                    "[WatchdogService][handleStart] Start request rejected because EC name is invalid"
                            + " | correlationId=" + request.getCorrelationId()
            );

            return createErrorResponse(
                    request,
                    "UNABLE_TO_START",
                    "Invalid process name"
            );
        }

        ProcessHandle processCheck = getProcess(ecName);

        if (processCheck != null && processCheck.isAlive()) {

            log.warn(() ->
                    "[WatchdogService][handleStart] Start request rejected because EC is already running"
                            + " | ecName=" + ecName
                            + " | pid=" + processCheck.pid()
            );

            return createErrorResponse(
                    request,
                    "UNABLE_TO_START",
                    "The container is already running."
            );
        }

        if (processCheck != null) {
            processes.remove(ecName, processCheck);
            log.debug(() ->
                    "[WatchdogService][handleStart] Removed stale process registration"
                            + " | ecName=" + ecName
                            + " | pid=" + processCheck.pid()
            );
        }

        String ecConfigPath = ecConfigPaths.get(ecName);

        if (ecConfigPath == null || ecConfigPath.isBlank()) {

            log.warn(() ->
                    "[WatchdogService][handleStart] Start request rejected because no EC configuration path is registered"
                            + " | ecName=" + ecName
            );

            return createErrorResponse(
                    request,
                    "UNABLE_TO_START",
                    "No configuration path is registered for EC: " + ecName
            );
        }

        try {

            log.info(() ->
                    "[WatchdogService][handleStart] Launching EC process"
                            + " | ecName=" + ecName
            );

            sendProgressResponse(
                    request,
                    "Configuration validated for EC " + ecName
                            + ". Launching process."
            );

            Process process = new ContainerLauncher().launchEC(
                    ecConfigPath,
                    List.of()
            );

            if (process == null) {

                log.error(
                        "[WatchdogService][handleStart] Container launcher returned a null process"
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
                    "[WatchdogService][handleStart] EC process created"
                            + " | ecName=" + ecName
                            + " | pid=" + launchedProcess.pid()
            );

            sendProgressResponse(
                    request,
                    "EC " + ecName
                            + " process created. PID: " + launchedProcess.pid()
                            + ". Verifying that the process is running."
            );

            if (!launchedProcess.isAlive()) {

                int exitCode = launchedProcess.exitValue();

                log.error(
                        "[WatchdogService][handleStart] EC process exited immediately"
                                + " | ecName=" + ecName
                                + " | pid=" + launchedProcess.pid()
                                + " | exitCode=" + exitCode
                );

                process = null;

                return createErrorResponse(
                        request,
                        "UNABLE_TO_START",
                        "The container process exited immediately. Exit code: "
                                + exitCode
                );
            }

            log.info(() ->
                    "[WatchdogService][handleStart] EC process is running and will be registered with WATCHDOG"
                            + " | ecName=" + ecName
                            + " | pid=" + launchedProcess.pid()
                            + " | containerType=" + ContainerType.WATCHDOG
            );

            sendProgressResponse(
                    request,
                    "EC " + ecName
                            + " process is running. Registering process with WATCHDOG."
            );

            Map<String, String> response = new HashMap<>();

            response.put(
                    "body",
                    "Process started. PID: " + launchedProcess.pid()
            );
            response.put(
                    "status",
                    "Process started. PID: " + launchedProcess.pid()
            );

            processes.put(
                    ecName,
                    launchedProcess.toHandle()
            );

            log.debug(() ->
                    "[WatchdogService][handleStart] EC process registered"
                            + " | ecName=" + ecName
                            + " | pid=" + launchedProcess.pid()
                            + " | trackedProcesses=" + processes.size()
            );

            return createResponseMessage(
                    request,
                    response
            );

        } catch (IOException | SecurityException exception) {

            log.error(
                    "[WatchdogService][handleStart] Unable to start EC process"
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

    private synchronized Message handleStop(Message request) {

        log.info(() ->
                "[WatchdogService][handleStop] Stop request received"
                        + " | correlationId=" + request.getCorrelationId()
        );

        String ecName = getProcessFromRequest(request);

        if (ecName == null) {

            log.warn(() ->
                    "[WatchdogService][handleStop] Stop request rejected because EC name is invalid"
                            + " | correlationId=" + request.getCorrelationId()
            );

            return createErrorResponse(
                    request,
                    "UNABLE_TO_KILL",
                    "Invalid process name"
            );
        }

        ProcessHandle processCheck = getProcess(ecName);

        if (processCheck == null || !processCheck.isAlive()) {

            if (processCheck != null) {
                processes.remove(ecName, processCheck);
            }

            log.info(() ->
                    "[WatchdogService][handleStop] EC process is already stopped"
                            + " | ecName=" + ecName
                            + " | staleRegistrationRemoved=" + (processCheck != null)
            );

            Map<String, String> response = new HashMap<>();

            response.put(
                    "body",
                    "Process is already stopped."
            );

            response.put(
                    "status",
                    "Process is already stopped."
            );

            return createResponseMessage(
                    request,
                    response
            );
        }

        ProcessHandle processToStop = processCheck;

        long processId = processToStop.pid();

        log.info(() ->
                "[WatchdogService][handleStop] Initiating graceful EC termination"
                        + " | ecName=" + ecName
                        + " | pid=" + processId
        );

        try {

            sendProgressResponse(
                    request,
                    "Sending graceful termination signal to EC "
                            + ecName + ". PID: " + processId
            );

            processToStop.destroy();

            sendProgressResponse(
                    request,
                    "Graceful termination signal sent. Waiting up to 15 seconds for EC "
                            + ecName + " to terminate."
            );

            boolean terminated = awaitTermination(processToStop, 15);

            if (!terminated) {

                log.warn(() ->
                        "[WatchdogService][handleStop] Graceful termination timed out; forcing termination"
                                + " | ecName=" + ecName
                                + " | pid=" + processId
                );

                sendProgressResponse(
                        request,
                        "Graceful termination timed out. Sending forced termination signal to EC "
                                + ecName + ". PID: " + processId
                );

                processToStop.destroyForcibly();

                sendProgressResponse(
                        request,
                        "Forced termination signal sent. Waiting up to 5 seconds for EC "
                                + ecName + " to terminate."
                );

                terminated = awaitTermination(processToStop, 5);
            }

            if (!terminated) {

                log.error(
                        "[WatchdogService][handleStop] Unable to terminate EC process after forced termination"
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

            processes.remove(ecName, processToStop);

            log.info(() ->
                    "[WatchdogService][handleStop] EC process stopped and registration removed"
                            + " | ecName=" + ecName
                            + " | pid=" + processId
                            + " | trackedProcesses=" + processes.size()
            );

            Map<String, String> response = new HashMap<>();

            response.put(
                    "body",
                    "Process stopped. PID: " + processId
            );

            response.put(
                    "status",
                    "Process stopped. PID: " + processId
            );

            return createResponseMessage(
                    request,
                    response
            );

        } catch (InterruptedException exception) {

            Thread.currentThread().interrupt();

            log.warn(() ->
                    "[WatchdogService][handleStop] Interrupted while waiting for EC process termination"
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
                    "[WatchdogService][handleStop] Unable to terminate EC process"
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

    private boolean awaitTermination(ProcessHandle process, long seconds) throws InterruptedException {
        if (!process.isAlive()) {
            return true;
        }
        try {
            process.onExit().get(seconds, TimeUnit.SECONDS);
            return true;
        } catch (TimeoutException exception) {
            return !process.isAlive();
        } catch (ExecutionException exception) {
            throw new IllegalStateException("Unable to observe EC process termination", exception.getCause());
        }
    }

    private synchronized Message handleRestart(Message request) {

        log.info(() ->
                "[WatchdogService][handleRestart] Restart request received"
                        + " | correlationId=" + request.getCorrelationId()
        );

        String ecName = getProcessFromRequest(request);

        if (ecName == null) {

            log.warn(() ->
                    "[WatchdogService][handleRestart] Restart request rejected because EC name is invalid"
                            + " | correlationId=" + request.getCorrelationId()
            );

            return createErrorResponse(
                    request,
                    "UNABLE_TO_START",
                    "Invalid process name"
            );
        }

        ProcessHandle processCheck = getProcess(ecName);

        log.info(() ->
                "[WatchdogService][handleRestart] Stopping existing EC process before restart"
                        + " | ecName=" + ecName
                        + " | processRegistered=" + (processCheck != null)
                        + " | processAlive=" + (processCheck != null && processCheck.isAlive())
        );

        Message stopResponse = handleStop(request);

        if ((processCheck != null && processCheck.isAlive())
                || Thread.currentThread().isInterrupted()) {

            log.warn(() ->
                    "[WatchdogService][handleRestart] Restart aborted because the existing process remains active or the thread is interrupted"
                            + " | ecName=" + ecName
                            + " | processAlive=" + (processCheck != null && processCheck.isAlive())
                            + " | threadInterrupted=" + Thread.currentThread().isInterrupted()
            );

            return stopResponse;
        }

        log.info(() ->
                "[WatchdogService][handleRestart] Existing process stop phase completed; starting EC"
                        + " | ecName=" + ecName
        );

        return handleStart(request);
    }

    private Message createResponseMessage(
            Message request,
            Map<String, String> messageValues
    ) {

        Message response = messageParser.createResponse(
                request.getCorrelationId(),
                messageValues
        );

        response.setStatus("SUCCESS");
        response.setResponseComplete(true);

        log.debug(() ->
                "[WatchdogService][createResponseMessage] Successful response created"
                        + " | correlationId=" + request.getCorrelationId()
        );

        return response;
    }

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
                "[WatchdogService][createErrorResponse] Error response created"
                        + " | correlationId=" + request.getCorrelationId()
                        + " | errorCode=" + errorCode
        );

        return response;
    }

    private void sendResponse(
            Message response
    ) {
        try {
            Objects.requireNonNull(
                    response,
                    "Response cannot be null"
            );

            Objects.requireNonNull(
                    controlChannelCommunicator,
                    "ControlChannelCommunicator has not been initialized"
            ).sendResponse(response);

            log.debug(() ->
                    "[WatchdogService][sendResponse] Response submitted to control channel"
                            + " | correlationId=" + response.getCorrelationId()
            );

        } catch (RuntimeException exception) {
            log.error(
                    "[WatchdogService][sendResponse] Unable to submit response",
                    exception
            );
        }
    }

    private void sendProgressResponse(
            Message request,
            String status
    ) {

        Map<String, String> responseValues = new HashMap<>();

        responseValues.put(
                "body",
                status
        );

        responseValues.put(
                "status",
                status
        );

        Message response = messageParser.createResponse(
                request.getCorrelationId(),
                responseValues
        );

        response.setStatus("IN_PROGRESS");
        response.setResponseComplete(false);

        log.debug(() ->
                "[WatchdogService][sendProgressResponse] Sending progress update"
                        + " | correlationId=" + request.getCorrelationId()
                        + " | status=" + status
        );

        sendResponse(response);
    }

    private void handleRequest(
            Message request
    ) {
        Objects.requireNonNull(
                request,
                "Request cannot be null"
        );

        log.info(() ->
                "[WatchdogService][handleRequest] Control request received"
                        + " | correlationId=" + request.getCorrelationId()
                        + " | operation=" + request.getOperation()
        );

        if (request.getCorrelationId() == null) {

            log.warn(() ->
                    "[WatchdogService][handleRequest] Control request ignored because correlation ID is missing"
                            + " | operation=" + request.getOperation()
            );

            return;
        }

        Message response;

        try {

            if (request.getOperation() == null) {

                response = createErrorResponse(
                        request,
                        "INVALID_OPERATION",
                        "The request does not contain a valid operation."
                );

                log.warn(() ->
                        "[WatchdogService][handleRequest] Control request has no operation"
                                + " | correlationId=" + request.getCorrelationId()
                );

            } else {

                log.debug(() ->
                        "[WatchdogService][handleRequest] Dispatching watchdog operation"
                                + " | correlationId=" + request.getCorrelationId()
                                + " | operation=" + request.getOperation()
                );

                response = switch (request.getOperation()) {

                    case START -> handleStart(request);

                    case STOP -> handleStop(request);

                    case RESTART -> handleRestart(request);
                    case DIAGNOSTICS, RUN_GC -> {
                        if (diagnostics == null) yield createErrorResponse(request, "DIAGNOSTICS_UNAVAILABLE", "Diagnostics not initialized.");
                        yield diagnostics.handleRequest(request);
                    }

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
                    "[WatchdogService][handleRequest] Error processing control request"
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

        final Message completedResponse = response;

        log.debug(() ->
                "[WatchdogService][handleRequest] Control request processing completed"
                        + " | correlationId=" + request.getCorrelationId()
        );

        sendResponse(
                completedResponse
        );
    }

    @Override
    public void onRequest(
            Message request
    ) {
        Objects.requireNonNull(
                request,
                "Request cannot be null"
        );

        log.debug(() ->
                "[WatchdogService][onRequest] Submitting control request to WATCHDOG worker pool"
                        + " | correlationId=" + request.getCorrelationId()
                        + " | operation=" + request.getOperation()
        );

        try {
            submitToThreadPool(
                    () -> handleRequest(
                            request
                    )
            );

        } catch (RejectedExecutionException | IllegalStateException exception) {

            log.warn(() ->
                    "[WatchdogService][onRequest] Unable to submit control request because worker pool is unavailable"
                            + " | correlationId=" + request.getCorrelationId()
                            + " | operation=" + request.getOperation()
                            + " | reason=" + exception.getMessage()
            );

            if (request.getCorrelationId() != null) {
                sendResponse(
                        createErrorResponse(
                                request,
                                "WATCHDOG_UNAVAILABLE",
                                "WATCHDOG cannot process the request because its worker pool is unavailable."
                        )
                );
            }
        }
    }

    @Override
    public void onInfo(Message request) {
        log.debug(() ->
                "[WatchdogService][onInfo] Informational control message received"
                        + " | correlationId=" + (request == null ? null : request.getCorrelationId())
        );
    }

    @Override
    public void onHeartbeat(Message response) {
        log.debug(() ->
                "[WatchdogService][onHeartbeat] Heartbeat received"
                        + " | correlationId=" + (response == null ? null : response.getCorrelationId())
        );
    }

    @Override
    public void onResponse(
            Message response
    ) {

        log.debug(() ->
                "[WatchdogService][onResponse] Response received"
                        + " | correlationId=" + (response == null ? null : response.getCorrelationId())
        );
    }

    @Override
    public void onTimeout(
            Message originalRequest
    ) {

        log.warn(() ->
                "[WatchdogService][onTimeout] Watchdog control request timed out"
                        + " | operation=" + originalRequest.getOperation()
                        + " | correlationId=" + originalRequest.getCorrelationId()
        );
    }

    public void start() {
        // A watchdog restart must recover supervision of existing ECs.
        for (String ecName : ecConfigPaths.keySet()) {
            getProcess(ecName);
        }
        log.info(() ->
                "[WatchdogService][start] Starting WATCHDOG worker pool and transport service"
                        + " | trackedProcesses=" + processes.size()
        );

        watchdogWorkers = new ThreadPool().initializeAndGetThreadPool(
                1, 1, 0, true
        );

        log.debug(() ->
                "[WatchdogService][start] WATCHDOG worker pool initialized"
                        + " | poolType=" + watchdogWorkers.getClass().getSimpleName()
        );

        try {
            Objects.requireNonNull(
                    service,
                    "Watchdog transport service has not been configured"
            ).start();

            log.info(() ->
                    "[WatchdogService][start] WATCHDOG transport service started"
            );

        } catch (RuntimeException exception) {
            log.error(
                    "[WatchdogService][start] Failed to start WATCHDOG transport service; shutting down worker pool",
                    exception
            );

            watchdogWorkers.shutdownNow();
            watchdogWorkers = null;
            throw exception;
        }
    }

    @Override
    public void shutdown() {
        log.info("[WatchdogService][shutdown] Preserving EC processes; only watchdog resources will stop");
        log.info(() ->
                "[WatchdogService][shutdown] WATCHDOG shutdown initiated"
                        + " | trackedProcesses=" + processes.size()
        );

        boolean isTerminated = false;
        if (service != null) {
            log.debug(() ->
                    "[WatchdogService][shutdown] Shutting down transport service"
                            + " | serviceType=" + service.getClass().getSimpleName()
            );
            service.shutdown();
        }

        if (watchdogWorkers != null) {
            watchdogWorkers.shutdown();

            log.debug(() ->
                    "[WatchdogService][shutdown] WATCHDOG worker pool shutdown requested"
            );

            try {
                if (!watchdogWorkers.awaitTermination(
                        5,
                        TimeUnit.SECONDS
                )) {
                    log.warn(() ->
                            "[WatchdogService][shutdown] Worker pool did not terminate within timeout; forcing shutdown"
                    );

                    watchdogWorkers.shutdownNow();
                    if (watchdogWorkers.isShutdown())
                        isTerminated = true;
                } else {
                    log.debug(() ->
                            "[WatchdogService][shutdown] WATCHDOG worker pool terminated gracefully"
                    );
                }

            } catch (InterruptedException exception) {
                log.warn(() ->
                        "[WatchdogService][shutdown] Interrupted while awaiting worker pool termination"
                );

                Thread.currentThread().interrupt();
                watchdogWorkers.shutdownNow();
            }
        } else {
            log.debug(() ->
                    "[WatchdogService][shutdown] WATCHDOG worker pool was not initialized"
            );
        }

        if (isTerminated) {
            watchdogWorkers = null;
        }

        final boolean workerPoolReferenceCleared = isTerminated;

        log.info(() ->
                "[WatchdogService][shutdown] WATCHDOG shutdown sequence completed"
                        + " | workerPoolReferenceCleared=" + workerPoolReferenceCleared
                        + " | workerPoolPresent=" + (watchdogWorkers != null)
        );
    }

    @Override
    public void quiesce() {
        log.info(() ->
                "[WatchdogService][quiesce] Quiescing WATCHDOG transport service"
        );

        if (service != null) {
            service.quiesce();
        }
    }

    @Override
    public void drain(long timeout) {
        log.info(() ->
                "[WatchdogService][drain] Draining WATCHDOG transport service"
                        + " | timeout=" + timeout
        );

        if (service != null) {
            service.drain(timeout);
        }
    }

    @Override
    public void cleanup() {
        int processCountBeforeCleanup = processes.size();

        processes.entrySet().removeIf(
                entry -> !entry.getValue().isAlive()
        );

        int processCountAfterCleanup = processes.size();

        log.debug(() ->
                "[WatchdogService][cleanup] Removed stale process registrations"
                        + " | trackedBefore=" + processCountBeforeCleanup
                        + " | trackedAfter=" + processCountAfterCleanup
                        + " | removed=" + (processCountBeforeCleanup - processCountAfterCleanup)
        );
    }

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
            log.warn(() ->
                    "[WatchdogService][submitToThreadPool] Submission rejected because WATCHDOG worker pool is shut down"
            );

            throw new RejectedExecutionException(
                    "WATCHDOG worker pool is shut down"
            );
        }

        workers.submit(runnable);

        log.debug(() ->
                "[WatchdogService][submitToThreadPool] Task submitted to WATCHDOG worker pool"
                        + " | poolShutdown=" + workers.isShutdown()
        );
    }
}
