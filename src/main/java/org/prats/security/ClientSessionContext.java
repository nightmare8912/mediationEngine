package org.prats.security;

/** Transport-local session state; never deserialized from a client message. */
public final class ClientSessionContext {
    private volatile String token;
    private volatile boolean closed;
    public String token() { return token; }
    public boolean isClosed() { return closed; }
    public synchronized void authenticate(String token) {
        if (closed) throw new IllegalArgumentException("Client connection is closed.");
        this.token = token;
    }
    public synchronized void close() { closed = true; }
    public synchronized void clear() { token = null; }
}
