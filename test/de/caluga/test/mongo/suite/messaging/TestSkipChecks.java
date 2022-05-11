package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.Test;

import java.util.Map;

import static de.caluga.morphium.StatisticKeys.PULLSKIP;
import static de.caluga.morphium.StatisticKeys.SKIPPED_MSG_UPDATES;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestSkipChecks extends MorphiumTestBase {
    @Test
    public void testStartup() throws Exception {
        checkStartup(100, false, false, 10);
        checkStartup(100, false, true, 100);
        checkStartup(100, true, true, 100);
        checkStartup(100, true, false, 100);
    }

    private void checkStartup(int pause, boolean multiple, boolean multith, int window) throws InterruptedException {
        morphium.resetStatistics();
        Messaging m = new Messaging(morphium, pause, multiple, multith, window);
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

        assertThat(pullskip).isEqualTo(stats.get(PULLSKIP.name()));
        assertThat(pullskip).isEqualTo(0);

        assertThat(stats.get("SKIPPED_MSG_UPDATES")).isGreaterThan(0);
        assertThat(stats.get("SKIPPED_MSG_UPDATES")).isGreaterThan(skips);

        m.terminate();
    }

    @Test
    public void testActiveMessaging() throws Exception {
        morphium.resetStatistics();
        Messaging m = new Messaging(morphium, 100, true, true, 100);
        m.start();
        m.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                return null;
            }
        });
        Messaging m2 = new Messaging(morphium, 100, true, true, 100);
        m2.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
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

        assertThat(pullskip).isEqualTo(stats.get(PULLSKIP.name()));

        assertThat(stats.get("SKIPPED_MSG_UPDATES")).isGreaterThan(0);
        assertThat(stats.get("SKIPPED_MSG_UPDATES")).isGreaterThan(skips);

        m.terminate();
        m2.terminate();

    }


}
