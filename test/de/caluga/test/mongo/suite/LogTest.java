package de.caluga.test.mongo.suite;/**
 * Created by stephan on 17.07.15.
 */

import de.caluga.morphium.Logger;
import de.caluga.morphium.LoggerDelegate;
import de.caluga.morphium.LoggerRegistry;
import org.junit.Test;

/**
 * TODO: Add Documentation here
 **/
public class LogTest extends MongoTest {
    private static boolean logCalled = false;

    @Test
    public void loggerTest() {
        Logger l = new Logger("loggertest");
        String f = l.getFile();
        log.info("File: " + f);
        assert (f.equals("-"));
        assert (l.getLevel() > 1) : "Loglevel wrong: " + l.getLevel();
    }

    @Test
    public void logDelegateTest() {
        morphium.getConfig().setLogFileForPrefix("logDelegateTest", "class:" + TestLogDelegate.class.getName());
        Logger l = new Logger("logDelegateTest");
        assert (l.getFile().equals("class:" + TestLogDelegate.class.getName()));
        l.fatal("Just a testmessage");
        assert (logCalled);

    }


    @Test
    public void logPrefixTest() {
        morphium.getConfig().setLogFileForPrefix("test", "class:" + TestLogDelegate.class.getName());
        Logger l = new Logger("test.my.class");
        assert (l.getFile().equals("class:" + TestLogDelegate.class.getName()));

        l = new Logger("test.other");
        assert (l.getFile().equals("class:" + TestLogDelegate.class.getName()));

        l = new Logger("not_test");
        assert (!l.getFile().equals("class:" + TestLogDelegate.class.getName()));
        assert (l.getFile().equals("-"));
    }

    @Test
    public void updateLogTestSettings() throws Exception {
        Logger l = new Logger("test.class");
        int lv = l.getLevel();

        if (lv < 5) {
            lv++;
        } else {
            lv = 1;
        }

        l.fatal("Log level: " + lv);
        morphium.getConfig().setGlobalLogLevel(lv);
        assert (l.getLevel() == lv);
        l.fatal("Level is now: " + lv);
    }

    @Test
    public void logRegistryTest() throws Exception {
        int num = LoggerRegistry.get().getNumberOfRegisteredLoggers();
        log.info("Registered: " + num);
        for (int i = 0; i < 10; i++) new Logger("prefix");
        int num2 = LoggerRegistry.get().getNumberOfRegisteredLoggers();
        log.info("Registered: " + num2);
        System.gc();
        int num3 = LoggerRegistry.get().getNumberOfRegisteredLoggers();
        log.info("Registered: " + num3);
    }

    public static class TestLogDelegate implements LoggerDelegate {

        @Override
        public void log(String loggerName, int lv, String msg, boolean synced, Throwable t) {
            logCalled = true;
        }
    }
}
