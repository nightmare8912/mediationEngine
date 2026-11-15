package org.prats.desktop.pages;

import org.prats.desktop.components.FormField;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.event.ActionListener;

public class LoginPage extends JPanel {

    private final FormField<JTextField> platformHostTF =
            new FormField<>(
                    "Platform Host",
                    new JTextField(24)
            );

    private final FormField<JTextField> usernameTF =
            new FormField<>(
                    "Username",
                    new JTextField(24)
            );

    private final FormField<JPasswordField> passwordTF =
            new FormField<>(
                    "Password",
                    new JPasswordField(24)
            );

    private final JButton loginBTN = new JButton("Login");

    public LoginPage() {

        setLayout(
                new BoxLayout(this, BoxLayout.Y_AXIS)
        );

        setBorder(
                new EmptyBorder(24, 24, 24, 24)
        );

        add(platformHostTF);

        add(Box.createVerticalStrut(12));

        add(usernameTF);

        add(Box.createVerticalStrut(12));

        add(passwordTF);

        add(Box.createVerticalStrut(20));

        add(loginBTN);
    }

    public void addLoginListener(ActionListener listener) {
        loginBTN.addActionListener(listener);
    }

    public String getPlatformHost() {
        return platformHostTF.getComponent().getText();
    }

    public String getUsername() {
        return usernameTF.getComponent().getText();
    }

    public char[] getPassword() {
        return passwordTF.getComponent().getPassword();
    }
}