package org.prats.platform.tcpService;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;

public class ClientConnection {
    private final Socket socket;
    private final BufferedWriter writer;

    public ClientConnection (Socket socket)
            throws IOException {
        this.socket = socket;
        this.writer = new BufferedWriter(
                new OutputStreamWriter(
                        socket.getOutputStream()
                )
        );
    }

    public synchronized void sendMessage(String message)
            throws IOException {
        writer.write(message);
        writer.newLine();
        writer.flush();
    }

    public Socket getSocket() {
        return socket;
    }
}
