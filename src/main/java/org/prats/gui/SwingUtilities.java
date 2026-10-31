
package org.prats.gui;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;

public class SwingUtilities {

    public SwingUtilities() { }

    // CREATE SCREEN
    public JFrame createScreen(
            int width, int height, String title) {

        JFrame frame = new JFrame(title);

        frame.setSize(width, height);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setLayout(null);
        frame.setLocationRelativeTo(null);

        return frame;
    }

    // CREATE PANEL
    public JPanel createPanel(
            int x, int y, int width, int height) {

        JPanel panel = new JPanel();

        panel.setBounds(x, y, width, height);
        panel.setLayout(null);

        return panel;
    }

    // CREATE LABEL
    public JLabel createLabel(
            int x, int y, int width, int height, String text) {

        JLabel label = new JLabel();

        label.setBounds(x, y, width, height);
        label.setText(text);

        return label;
    }

    // CREATE TEXT FIELD
    public JTextField createTextField(
            int x, int y, int width, int height, String text) {

        JTextField textField = new JTextField();

        textField.setBounds(x, y, width, height);
        textField.setText(text);

        return textField;
    }

    // CREATE COMBO BOX
    public JComboBox<String> createComboBox(
            int x, int y, int width, int height,
            String[] options, String defaultSelected) {

        JComboBox<String> comboBox = new JComboBox<>(options);

        comboBox.setBounds(x, y, width, height);

        if (defaultSelected != null) {
            comboBox.setSelectedItem(defaultSelected);
        }

        return comboBox;
    }

    // CREATE CHECK BOX
    public JCheckBox createCheckBox(
            int x, int y, int width, int height,
            String text, boolean selected) {

        JCheckBox checkBox = new JCheckBox(text);

        checkBox.setBounds(x, y, width, height);
        checkBox.setSelected(selected);

        return checkBox;
    }

    // CREATE BUTTON
    public JButton createButton(
            int x, int y, int width, int height, String text) {

        JButton button = new JButton(text);

        button.setBounds(x, y, width, height);

        return button;
    }

    // CREATE TABLE
    public JTable createTable(
            String[] columnNames, Object[][] data) {

        DefaultTableModel model =
                new DefaultTableModel(data, columnNames);

        JTable table = new JTable(model);

        return table;
    }

    // CREATE TABLE SCROLL PANE
    public JScrollPane createTableScrollPane(
            JTable table, int x, int y, int width, int height) {

        JScrollPane scrollPane = new JScrollPane(table);

        scrollPane.setBounds(x, y, width, height);

        return scrollPane;
    }

    // CREATE MENU BAR
    public JMenuBar createMenuBar() {

        JMenuBar menuBar = new JMenuBar();

        return menuBar;
    }

    // CREATE MENU
    public JMenu createMenu(String menuName) {

        JMenu menu = new JMenu(menuName);

        return menu;
    }

    // CREATE MENU ITEM
    public JMenuItem createMenuItem(String itemName) {

        JMenuItem menuItem = new JMenuItem(itemName);

        return menuItem;
    }

    // CREATE TABBED PANE
    public JTabbedPane createTabbedPane(
            int x, int y, int width, int height) {

        JTabbedPane tabbedPane = new JTabbedPane();

        tabbedPane.setBounds(x, y, width, height);

        return tabbedPane;
    }

    // ADD TAB
    public void addTab(
            JTabbedPane tabbedPane, String tabName, JPanel panel) {

        tabbedPane.addTab(tabName, panel);
    }

}
