package org.prats.definitions;

import java.util.List;

public class DatabaseDefinition {
    private String label;
    private String id;
    private String url;
    private String user;
    private String pass;
    private long timeout;
    private long idleTimeout;
    private int maxConnections;
    private List<DatabaseTableDefinition> tables;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public List<DatabaseTableDefinition> getTables() {
        return tables;
    }

    public void setTables(List<DatabaseTableDefinition> tables) {
        this.tables = tables;
    }
}
