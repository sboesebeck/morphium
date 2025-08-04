package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.StdMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static de.caluga.morphium.StatisticKeys.PULLSKIP;
import static de.caluga.morphium.StatisticKeys.SKIPPED_MSG_UPDATES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
public class TestSkipChecks extends MorphiumTestBase {
    @Test
    public void testStartup() throws Exception {
        checkStartup(100, false, false, 10);
        checkStartup(100, false, true, 100);
        checkStartup(100, true, true, 100);
        checkStartup(100, true, false, 100);
    }

    private void checkStartup(int pause, boolean multiple, boolean multith, int window)  throws Exception {
        morphium.resetStatistics();
        StdMessaging m = new StdMessaging(morphium, pause, multiple, multith, window);
        m.start();
        Thread.sleep(100);
        Map<String, Double> stats = morphium.getStatistics();
        double pullskip = stats.get("PULLSKIP");
        log.info("Pullskip (checks forced): " + pullskip);
        log.info("Checks skipped          : " + stats.get(SKIPPED_MSG_UPDATES.name()));
        double skips = stats.get(SKIPPED_MSG_UPDATES.name());
        Thread.sleep(500);
        stats = morphium.getStatistics();
        log.info("Pullskip (checks forced): " + stats.get(PULLSKIP.name()));
        log.info("Checks skipped          : " + stats.get(SKIPPED_MSG_UPDATES.name()));

        assertEquals(stats.get(PULLSKIP.name()), pullskip);
        assertEquals(0, pullskip);

        assertTrue(stats.get("SKIPPED_MSG_UPDATES") > 0);
        assertTrue(stats.get("SKIPPED_MSG_UPDATES") > skips);

        m.terminate();
    }

    @Test
    public void testActiveMessaging() throws Exception {
        morphium.resetStatistics();
        StdMessaging m = new StdMessaging(morphium, 100, true, true, 100);
        m.start();
        m.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(MorphiumMessaging msg, Msg m) {
                return null;
            }
        });
        StdMessaging m2 = new StdMessaging(morphium, 100, true, true, 100);
        m2.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(MorphiumMessaging msg, Msg m) {
                return null;
            }
        });
        m2.start();

        m2.sendMessage(new Msg("test", "whatever", "value"));
        m.sendMessage(new Msg("other", "whatever", "42"));


        Thread.sleep(500);
        Map<String, Double> stats = morphium.getStatistics();
        double pullskip = stats.get("PULLSKIP");
        log.info("Pullskip (checks forced): " + pullskip);
        log.info("Checks skipped          : " + stats.get(SKIPPED_MSG_UPDATES.name()));
        double skips = stats.get(SKIPPED_MSG_UPDATES.name());
        Thread.sleep(500);
        stats = morphium.getStatistics();
        log.info("Pullskip (checks forced): " + stats.get(PULLSKIP.name()));
        log.info("Checks skipped          : " + stats.get(SKIPPED_MSG_UPDATES.name()));

        assertEquals(stats.get(PULLSKIP.name()), pullskip);

        assertTrue(stats.get("SKIPPED_MSG_UPDATES") > 0);
        assertTrue(stats.get("SKIPPED_MSG_UPDATES") > skips);

        m.terminate();
        m2.terminate();

    }


}
