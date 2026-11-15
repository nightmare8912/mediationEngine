package org.prats.processors;

import org.prats.definitions.interfaces.ExternalChanelCommunicator;
import org.prats.parsers.CommandParser;

public class PlatformWatchdogProcessor {

    private final CommandParser commandParser;
    private final ExternalChanelCommunicator externalChanelCommunicator;

    public PlatformWatchdogProcessor(
            CommandParser commandParser,
            ExternalChanelCommunicator externalChanelCommunicator
    ) {
        this.commandParser              = commandParser;
        this.externalChanelCommunicator = externalChanelCommunicator;
    }

    public void processStartEC(String ecName, String watchdogName) {

    }
}
