package org.prats.desktop;

import org.prats.definitions.classes.ProcessContext;
import org.prats.definitions.enums.ProcessType;
import org.prats.desktop.controllers.LoginController;
import org.prats.desktop.pages.LoginPage;
import org.prats.parsers.MessageParser;

import javax.swing.JFrame;
import javax.swing.SwingUtilities;

public final class DesktopApplication {

    private final static DesktopConnectionManager desktopConnectionManager
            = new DesktopConnectionManager();

    private final static MessageParser messageParser
            = new MessageParser(ProcessType.GUI);

    private final static String PROCESS_NAME
            = "GUI";

    public static void main(String[] args) {

        ProcessContext.initialize(PROCESS_NAME);

        SwingUtilities.invokeLater(() -> {

            JFrame frame = new JFrame(
                    "Mediation Engine"
            );


            frame.setDefaultCloseOperation(
                    JFrame.EXIT_ON_CLOSE
            );

            LoginPage loginPage =
                    new LoginPage();

            frame.setContentPane(loginPage);

            frame.pack();

            frame.setLocationRelativeTo(null);


            DesktopConnectionManager desktopConnectionManager
                    = new DesktopConnectionManager();

            LoginController loginController
                    = new LoginController(
                            loginPage,
                            desktopConnectionManager,
                            messageParser
            );

            frame.setVisible(true);

        });

    }
}