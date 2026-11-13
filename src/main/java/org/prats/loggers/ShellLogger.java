package org.prats.loggers;

public class ShellLogger {
    private static boolean isLoggingEnabled = true;

//    public FileLogger() {
//        isLoggingEnabled = true;
//    }

    public static void log(String message) {
        if (isLoggingEnabled)
            System.out.println(message);
    }
}
