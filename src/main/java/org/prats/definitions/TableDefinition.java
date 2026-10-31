package org.prats.definitions;

import java.util.List;

public class TableDefinition {
    private String label;
    private String id;
    private int numberOfFields;
    List<TableFieldDefinition> fields;

    public int getNumberOfFields() {
        return numberOfFields;
    }

    public void setNumberOfFields(int numberOfFields) {
        this.numberOfFields = numberOfFields;
    }

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

    public List<TableFieldDefinition> getFields() {
        return fields;
    }

    public void setFields(List<TableFieldDefinition> fields) {
        this.fields = fields;
    }
}
