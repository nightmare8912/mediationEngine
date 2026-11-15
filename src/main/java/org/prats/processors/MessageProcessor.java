package org.prats.processors;

import org.prats.definitions.classes.Message;
import org.prats.definitions.interfaces.Forwarder;
import org.prats.forwarders.CommandForwarder;
import org.prats.loggers.EngineLogger;

import java.util.Objects;

public class MessageProcessor {

    private static final EngineLogger log =
            EngineLogger.getLogger(MessageProcessor.class);

    private final Forwarder databaseForwarder;

    private CommandForwarder commandForwarder;
    private Forwarder guiForwarder;
    private Forwarder authenticationForwarder;
    private Forwarder diagnosticsForwarder;

    public void setAuthenticationForwarder(Forwarder forwarder) { authenticationForwarder = Objects.requireNonNull(forwarder); }
    public void setDiagnosticsForwarder(Forwarder forwarder) { diagnosticsForwarder = Objects.requireNonNull(forwarder); }


    public void setGUIForwarder(Forwarder forwarder) { this.guiForwarder = Objects.requireNonNull(forwarder); }

    public MessageProcessor(Forwarder databaseForwarder) {
        this.databaseForwarder =
                databaseForwarder;

        log.debug(() ->
                "[MessageProcessor][constructor] Message processor initialized"
                        + " | databaseForwarder="
                        + (databaseForwarder == null
                        ? "null"
                        : databaseForwarder.getClass().getSimpleName())
        );
    }

    public void setCommandForwarder(CommandForwarder commandForwarder) {

        this.commandForwarder = Objects.requireNonNull(
                commandForwarder,
                "Command forwarder cannot be null"
        );

        log.debug(() ->
                "[MessageProcessor][setCommandForwarder] Command forwarder configured"
                        + " | forwarderType="
                        + commandForwarder.getClass().getSimpleName()
        );
    }

    public void process(Message message) {

        switch (message.getOperation()) {
            case AUTHENTICATE -> {
                if (authenticationForwarder == null) throw new IllegalStateException("Authentication route unavailable");
                authenticationForwarder.forward(message);
            }
            case DIAGNOSTICS, RUN_GC -> {
                if (diagnosticsForwarder == null) throw new IllegalStateException("Diagnostics route unavailable");
                diagnosticsForwarder.forward(message);
            }
            case GUI -> {
                if (guiForwarder == null) throw new IllegalStateException("GUI requests are supported only by PLATFORM");
                guiForwarder.forward(message);
            }

            case FETCH, SEARCH, CREATE, UPDATE, DELETE -> {
                log.debug(() ->
                        "[MessageProcessor][process] Forwarding database operation"
                                + " | operation=" + message.getOperation()
                );

                databaseForwarder.forward(message);

                log.debug(() ->
                        "[MessageProcessor][process] Database operation forwarded"
                                + " | operation=" + message.getOperation()
                );
            }

            case COMMAND -> {

                if (commandForwarder == null) {
                    log.error(
                            "[MessageProcessor][process] Command forwarder has not been initialized"
                    );

                    throw new IllegalStateException(
                            "Command forwarder has not been initialized"
                    );
                }

                log.debug(
                        "[MessageProcessor][process] Forwarding command operation"
                );

                commandForwarder.forward(message);

                log.debug(
                        "[MessageProcessor][process] Command operation forwarded"
                );
            }
            default -> throw new IllegalArgumentException("Unsupported operation: " + message.getOperation());
        }
    }
}
