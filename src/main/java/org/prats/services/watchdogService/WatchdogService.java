package org.prats.services.watchdogService;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.prats.container.launchers.ContainerLauncher;
import org.prats.definitions.classes.Message;
import org.prats.definitions.enums.ContainerType;
import org.prats.definitions.interfaces.ControlMessageHandler;
import org.prats.definitions.interfaces.Service;
import org.prats.definitions.records.ConsolidatedECConfig;
import org.prats.definitions.records.Watchdog;
import org.prats.jvmLauncher.JVMLauncher;
import org.prats.loggers.FileLogger;
import org.prats.parsers.MessageParser;
import org.prats.definitions.interfaces.ControlChannelCommunicator;
import org.prats.threading.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ConcurrentHashMap;

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

    private final MessageParser messageParser;
    private final Map<String, String> ecConfigPaths;;
    private ExecutorService watchdogWorkers;
    private Service service;

    private ControlChannelCommunicator controlChannelCommunicator;

    private Map<String, Process> processes;

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
    }

    public void setService(Service service) {
        this.service = service;
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
            FileLogger.log(
                    "[WatchdogService][getProcessFromRequest]: "
                            + jsonProcessingException.getMessage()
            );
            processName = null;
        }
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
    }

    private Process getProcess(String processName) {
        return processes.get(processName);
    }

    private synchronized Message handleStart(Message request) {

        String ecName = getProcessFromRequest(request);

        if (ecName == null) {

            return createErrorResponse(
                    request,
                    "UNABLE_TO_START",
                    "Invalid process name"
            );
        }

        Process processCheck = getProcess(ecName);

        if (processCheck != null && processCheck.isAlive()) {

            return createErrorResponse(
                    request,
                    "UNABLE_TO_START",
                    "The container is already running."
            );
        }

        if (processCheck != null) {
            processes.remove(ecName, processCheck);
        }

        String ecConfigPath = ecConfigPaths.get(ecName);

        if (ecConfigPath == null || ecConfigPath.isBlank()) {

            return createErrorResponse(
                    request,
                    "UNABLE_TO_START",
                    "No configuration path is registered for EC: " + ecName
            );
        }

        try {

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

                return createErrorResponse(
                        request,
                        "UNABLE_TO_START",
                        "Container launcher returned a null process."
                );
            }

            sendProgressResponse(
                    request,
                    "EC " + ecName
                            + " process created. PID: " + process.pid()
                            + ". Verifying that the process is running."
            );

            if (!process.isAlive()) {

                int exitCode = process.exitValue();

                FileLogger.log(
                        "[WatchdogService] [handleStart] "
                                + "Container exited immediately. Exit code: "
                                + exitCode
                );

                process = null;

                return createErrorResponse(
                        request,
                        "UNABLE_TO_START",
                        "The container process exited immediately. Exit code: "
                                + exitCode
                );
            }

            FileLogger.log(
                    "[WatchdogService] Started "
                            + ContainerType.WATCHDOG
                            + ". PID: "
                            + process.pid()
            );

            sendProgressResponse(
                    request,
                    "EC " + ecName
                            + " process is running. Registering process with WATCHDOG."
            );

            Map<String, String> response = new HashMap<>();

            response.put(
                    "body",
                    "Process started. PID: " + process.pid()
            );
            response.put(
                    "status",
                    "Process started. PID: " + process.pid()
            );

            processes.put(
                    ecName,
                    process
            );

            return createResponseMessage(
                    request,
                    response
            );

        } catch (IOException | SecurityException exception) {

            FileLogger.log(
                    "[WatchdogService] [handleStart] "
                            + "Unable to start container: "
                            + exception
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

        String ecName = getProcessFromRequest(request);

        if (ecName == null) {

            return createErrorResponse(
                    request,
                    "UNABLE_TO_KILL",
                    "Invalid process name"
            );
        }

        Process processCheck = getProcess(ecName);

        if (processCheck == null || !processCheck.isAlive()) {

            if (processCheck != null) {
                processes.remove(ecName, processCheck);
            }

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

        Process processToStop = processCheck;

        long processId = processToStop.pid();

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

            boolean terminated = processToStop.waitFor(
                    15,
                    TimeUnit.SECONDS
            );

            if (!terminated) {

                FileLogger.log(
                        "[WatchdogService] [handleStop] "
                                + "Graceful termination timed out. "
                                + "Forcibly terminating PID: "
                                + processId
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

                terminated = processToStop.waitFor(
                        5,
                        TimeUnit.SECONDS
                );
            }

            if (!terminated) {

                FileLogger.log(
                        "[WatchdogService] [handleStop] "
                                + "Unable to terminate PID: "
                                + processId
                );

                return createErrorResponse(
                        request,
                        "UNABLE_TO_KILL",
                        "Process could not be terminated. PID: "
                                + processId
                );
            }

            processes.remove(ecName, processToStop);

            FileLogger.log(
                    "[WatchdogService] Successfully stopped PID: "
                            + processId
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

            FileLogger.log(
                    "[WatchdogService] [handleStop] "
                            + "Interrupted while terminating PID: "
                            + processId
            );

            return createErrorResponse(
                    request,
                    "INTERRUPTED",
                    "Interrupted while waiting for process termination."
            );

        } catch (SecurityException exception) {

            FileLogger.log(
                    "[WatchdogService] [handleStop] "
                            + "Unable to terminate process: "
                            + exception
            );

            return createErrorResponse(
                    request,
                    "UNABLE_TO_KILL",
                    "Permission denied while terminating process: "
                            + exception.getMessage()
            );
        }
    }

    private synchronized Message handleRestart(Message request) {

        String ecName = getProcessFromRequest(request);

        if (ecName == null) {

            return createErrorResponse(
                    request,
                    "UNABLE_TO_START",
                    "Invalid process name"
            );
        }

        Process processCheck = getProcess(ecName);

        Message stopResponse = handleStop(request);

        if ((processCheck != null && processCheck.isAlive())
                || Thread.currentThread().isInterrupted()) {

            return stopResponse;
        }

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

        } catch (RuntimeException exception) {
            FileLogger.log(
                    "[WatchdogService] [sendResponse] Unable to submit response: "
                            + exception.getMessage()
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

        sendResponse(response);
    }

    private void handleRequest(
            Message request
    ) {
        Objects.requireNonNull(
                request,
                "Request cannot be null"
        );

        FileLogger.log(
                "[TCPControlProcessor] Received request: "
                        + request
        );

        if (request.getCorrelationId() == null) {

            FileLogger.log(
                    "[WatchdogService] [onRequest] "
                            + "Received request without correlation ID."
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

                FileLogger.log(
                        "[WatchdogService] The request does not contain a valid operation. "
                                + request
                );

            } else {

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

            FileLogger.log(
                    "[WatchdogService] [onRequest] "
                            + "Error processing control request: "
                            + exception
            );

            response = createErrorResponse(
                    request,
                    "WATCHDOG_COMMAND_FAILED",
                    "An unexpected error occurred while processing "
                            + "the watchdog command: "
                            + exception.getMessage()
            );
        }

        sendResponse(
                response
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

        try {
            submitToThreadPool(
                    () -> handleRequest(
                            request
                    )
            );

        } catch (RejectedExecutionException | IllegalStateException exception) {

            FileLogger.log(
                    "[WatchdogService] [onRequest] "
                            + "Unable to submit control request: "
                            + exception.getMessage()
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

    }

    @Override
    public void onHeartbeat(Message response) {

    }

    @Override
    public void onResponse(
            Message response
    ) {

        FileLogger.log(
                "[WatchdogService] Received response"
        );
    }

    @Override
    public void onTimeout(
            Message originalRequest
    ) {

        FileLogger.log(
                "[WatchdogService] Request timed out: "
                        + originalRequest.getOperation()
                        + ", correlation ID: "
                        + originalRequest.getCorrelationId()
        );
    }

    public void start() {
        watchdogWorkers = new ThreadPool().initializeAndGetThreadPool(
                1, 1, 0, true
        );

        try {
            Objects.requireNonNull(
                    service,
                    "Watchdog transport service has not been configured"
            ).start();

        } catch (RuntimeException exception) {
            watchdogWorkers.shutdownNow();
            watchdogWorkers = null;
            throw exception;
        }
    }

    @Override
    public void shutdown() {
        boolean isTerminated = false;
        if (service != null) {
            service.shutdown();
        }

        if (watchdogWorkers != null) {
            watchdogWorkers.shutdown();

            try {
                if (!watchdogWorkers.awaitTermination(
                        5,
                        TimeUnit.SECONDS
                )) {
                    watchdogWorkers.shutdownNow();
                    if (watchdogWorkers.isShutdown())
                        isTerminated = true;
                }

            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                watchdogWorkers.shutdownNow();
            }
        }

        if (isTerminated) {
            watchdogWorkers = null;
        }
    }

    @Override
    public void quiesce() {
        if (service != null) {
            service.quiesce();
        }
    }

    @Override
    public void drain(long timeout) {
        if (service != null) {
            service.drain(timeout);
        }
    }

    @Override
    public void cleanup() {
        processes.entrySet().removeIf(
                entry -> !entry.getValue().isAlive()
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
            throw new RejectedExecutionException(
                    "WATCHDOG worker pool is shut down"
            );
        }

        workers.submit(runnable);
    }
}