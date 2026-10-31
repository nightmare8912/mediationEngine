package org.prats.definitions;

import java.util.List;

public class DatabaseDefinition {
    private String label;
    private String id;
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

    public List<DatabaseFieldDefinition> getTables() {
        return tables;
    }

    public void setTables(List<DatabaseFieldDefinition> tables) {
        this.tables = tables;
    }
}
