package org.prats.messageHandler;

import org.prats.definitions.MessageDefinition;
import org.prats.definitions.ClientConnectionDefinition;
import org.prats.services.tcpService.TCPService;

public class TCPReplyChannel implements ReplyChannel{
    public final ClientConnectionDefinition clientConnectionDefinition;
    public final TCPService tcpService;

    public TCPReplyChannel(
            ClientConnectionDefinition clientConnectionDefinition,
            TCPService tcpService
    ) {
        this.clientConnectionDefinition = clientConnectionDefinition;
        this.tcpService                 = tcpService;
    }

    @Override
    public void sendMessage(String message) {
        tcpService.sendMessage(
                message,
                clientConnectionDefinition
        );
    }
}
