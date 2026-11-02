package org.prats.definitions;

public class TableField {

    private String id;
    private String label;
    private String type;
    private boolean required;
    private boolean primaryKey;

    public String getId() { return id; }

    public void setId(String id) {
        this.id = id;
    }

    public boolean getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean getRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }
}