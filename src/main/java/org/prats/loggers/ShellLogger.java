package org.prats.loggers;

public class ShellLogger {
    private static boolean isLogginEnabled = true;

    public ShellLogger() {
        isLogginEnabled = true;
    }

    public static void log(String message) {
        if (isLogginEnabled)
            System.out.println(message);
    }
}
