package org.prats.definitions;

import java.util.List;

public class TabDefinition {
    private String label;
    private List<FieldDefinition> fields;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public List<FieldDefinition> getFields() {
        return fields;
    }

    public void setFields(List<FieldDefinition> fields) {
        this.fields = fields;
    }
}