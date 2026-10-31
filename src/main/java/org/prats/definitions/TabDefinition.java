package org.prats.definitions;

import java.util.List;

public class TabDefinition {
    private String label;
    private List<TabFieldDefinition> fields;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public List<TabFieldDefinition> getFields() {
        return fields;
    }

    public void setFields(List<TabFieldDefinition> fields) {
        this.fields = fields;
    }
}