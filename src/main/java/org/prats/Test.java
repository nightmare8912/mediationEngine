package org.prats;

import org.prats.loggers.EngineLogger;

public class Test {

    private static final EngineLogger log =
            EngineLogger.getLogger(Test.class);

    public static void main(String[] args) {

        log.debug("DEBUG test");

        log.info("INFO test");

        log.warn("WARN test");

        try {

            throw new RuntimeException("Testing error logging");

        } catch (Exception e) {

            log.error("ERROR test", e);

        }
    }
}