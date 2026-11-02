package org.prats.definitions;

import java.util.List;

public class Tab {
    private String label;
    private List<TabField> fields;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public List<TabField> getFields() {
        return fields;
    }

    public void setFields(List<TabField> fields) {
        this.fields = fields;
    }
}