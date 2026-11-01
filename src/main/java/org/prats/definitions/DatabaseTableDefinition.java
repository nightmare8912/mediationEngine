package org.prats.definitions;

public class DatabaseTableDefinition {
    private String tableLabel;
    private String tableId;
    private int creationSequence;
    private boolean critical;
    private String path;

    public String getTableLabel() {
        return tableLabel;
    }

    public void setTableLabel(String tableLabel) {
        this.tableLabel = tableLabel;
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public int getCreationSequence() {
        return creationSequence;
    }

    public void setCreationSequence(int creationSequence) {
        this.creationSequence = creationSequence;
    }

    public boolean isCritical() {
        return critical;
    }

    public void setCritical(boolean critical) {
        this.critical = critical;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
