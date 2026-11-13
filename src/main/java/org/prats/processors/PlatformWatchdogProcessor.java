package org.prats.processors;

import org.prats.definitions.interfaces.ControlChannelCommunicator;
import org.prats.parsers.CommandParser;

public class PlatformWatchdogProcessor {

    private final CommandParser commandParser;
    private final ControlChannelCommunicator controlChannelCommunicator;

    public PlatformWatchdogProcessor(
            CommandParser commandParser,
            ControlChannelCommunicator controlChannelCommunicator
    ) {
        this.commandParser              = commandParser;
        this.controlChannelCommunicator = controlChannelCommunicator;
    }

    public void processStartEC(String ecName, String watchdogName) {

    }
}
