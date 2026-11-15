package org.prats.desktop.components;

import javax.swing.*;
import java.awt.BorderLayout;

public class FormField<T extends JComponent> extends JPanel {

    private final String label;
    private final T component;

    public FormField(String label, T component) {

        super(new BorderLayout(0, 8));

        this.label = label;
        this.component = component;

        JLabel fieldLabel = new JLabel(label);

        add(fieldLabel, BorderLayout.NORTH);
        add(component, BorderLayout.CENTER);
    }

    public String getLabel() {
        return label;
    }

    public T getComponent() {
        return component;
    }

}