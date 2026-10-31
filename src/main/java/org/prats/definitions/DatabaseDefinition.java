package org.prats.definitions;

import java.util.List;

public class DatabaseDefinition {
    private String label;
    private String id;
    private String url;
    private String user;
    private String pass;
    private List<DatabaseFieldDefinition> tables;

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

    public List<DatabaseFieldDefinition> getTables() {
        return tables;
    }

    public void setTables(List<DatabaseFieldDefinition> tables) {
        this.tables = tables;
    }
}
