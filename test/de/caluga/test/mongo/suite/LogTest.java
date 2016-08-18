package de.caluga.test.mongo.suite;/**
 * Created by stephan on 17.07.15.
 */

import de.caluga.morphium.Logger;
import de.caluga.morphium.LoggerDelegate;
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
        assert (l.getLevel() > 2);
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
        morphium.getConfig().setGlobalUpdateLogSettingsInterval(100);
        Logger l = new Logger("test.class");
        int lv = l.getLevel();

        if (lv < 5) {
            lv++;
        } else {
            lv = 1;
        }

        l.fatal("Log level: " + lv);
        morphium.getConfig().setGlobalLogLevel(lv);
        assert (l.getLevel() != lv);
        Thread.sleep(200);
        l.fatal("Logging again");
        assert (l.getLevel() == lv);
        l.fatal("Level is now: " + lv);
    }

    public static class TestLogDelegate implements LoggerDelegate {

        @Override
        public void log(String loggerName, int lv, String msg, Throwable t) {
            logCalled = true;
        }
    }
}
