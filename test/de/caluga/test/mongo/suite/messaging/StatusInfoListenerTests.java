package de.caluga.test.mongo.suite.messaging;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.StatusInfoListener;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class StatusInfoListenerTests extends MorphiumTestBase {

    @Test
    public void getStatusInfo() throws Exception {
        Messaging m1 = new Messaging(morphium, 100, true);
        m1.setSenderId("m1");
        m1.setMultithreadded(true);
        m1.start();
        Messaging m2 = new Messaging(morphium, 100, true);
        m2.setSenderId("m2");
        m2.setMultithreadded(false);
        m2.start();

        m1.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                return null;
            }
        });
        m1.addListenerForMessageNamed("test1", new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                return null;
            }
        });

        m2.addListenerForMessageNamed("test1", new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                return null;
            }
        });

        m2.addListenerForMessageNamed("test2", new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                return null;
            }
        });

        Thread.sleep(1500);
        Messaging sender = new Messaging(morphium, 100, true);
        sender.start();
        log.info("Getting standard stauts (should be Messaging only)");
        List<Msg> lst = sender.sendAndAwaitAnswers(new Msg(sender.getStatusInfoListenerName(), "status", "value"), 2, 5000);
        assertThat(lst).isNotNull();
        assertThat(lst.size()).isEqualTo(2);
        for (Msg m : lst) {
            Map<String, Object> mapValue = m.getMapValue();
            assertThat(mapValue.containsKey(StatusInfoListener.morphiumConfigKey)).isFalse();
            assertThat(mapValue.containsKey(StatusInfoListener.morphiumCachestatsKey)).isFalse();
            checkMessagingStats(m, mapValue);
        }
        log.info("Getting all status");

        lst = sender.sendAndAwaitAnswers(new Msg(sender.getStatusInfoListenerName(), "status", StatusInfoListener.StatusInfoLevel.ALL.name()), 2, 1000);
        assertThat(lst).isNotNull();
        assertThat(lst.size()).isEqualTo(2);
        for (Msg m : lst) {
            Map<String, Object> mapValue = m.getMapValue();
            assertThat(mapValue.containsKey(StatusInfoListener.morphiumConfigKey)).isTrue();
            assertThat(mapValue.containsKey(StatusInfoListener.morphiumCachestatsKey)).isTrue();
            checkMessagingStats(m, mapValue);
        }
        log.info("just ping");

        lst = sender.sendAndAwaitAnswers(new Msg(sender.getStatusInfoListenerName(), "status", StatusInfoListener.StatusInfoLevel.PING.name()), 2, 1000);
        assertThat(lst).isNotNull();
        assertThat(lst.size()).isEqualTo(2);
        for (Msg m : lst) {
            Map<String, Object> mapValue = m.getMapValue();
            assertThat(mapValue.size()).isEqualTo(0);
        }

        log.info("Getting morphium stats only");

        lst = sender.sendAndAwaitAnswers(new Msg(sender.getStatusInfoListenerName(), "status", StatusInfoListener.StatusInfoLevel.MORPHIUM_ONLY.name()), 2, 1000);
        assertThat(lst).isNotNull();
        assertThat(lst.size()).isEqualTo(2);
        for (Msg m : lst) {
            Map<String, Object> mapValue = m.getMapValue();
            assertThat(mapValue.containsKey(StatusInfoListener.morphiumConfigKey)).isTrue();
            assertThat(mapValue.containsKey(StatusInfoListener.morphiumCachestatsKey)).isTrue();
            assertThat(mapValue.containsKey(StatusInfoListener.messageListenersbyNameKey)).isFalse();
            assertThat(mapValue.containsKey(StatusInfoListener.globalListenersKey)).isFalse();
        }
        log.info("all find... exiting");

        m1.terminate();
        m2.terminate();
        sender.terminate();
    }

    private void checkMessagingStats(Msg m, Map<String, Object> mapValue) {
        if (m.getSender().equals("m1")) {
            assertThat(mapValue.containsKey(StatusInfoListener.messageListenersbyNameKey)).isTrue();
            assertThat(mapValue.containsKey(StatusInfoListener.globalListenersKey)).isTrue();
            assertThat(((List) mapValue.get(StatusInfoListener.globalListenersKey)).size()).isEqualTo(1);
            assertThat(((Map) mapValue.get(StatusInfoListener.messageListenersbyNameKey)).size()).isEqualTo(2); //own listener + statusinfo
            assertThat(((Map) mapValue.get(StatusInfoListener.messageListenersbyNameKey)).containsKey("test1")).isTrue();
            assertThat(((Map) mapValue.get(StatusInfoListener.messageListenersbyNameKey)).containsKey("test2")).isFalse();
            assertThat(mapValue.containsKey(StatusInfoListener.messagingThreadpoolstatsKey)).isTrue();

        } else if (m.getSender().equals("m2")) {
            assertThat(mapValue.containsKey(StatusInfoListener.messageListenersbyNameKey)).isTrue();
            assertThat(mapValue.containsKey(StatusInfoListener.globalListenersKey)).isTrue();
            assertThat(((List) mapValue.get(StatusInfoListener.globalListenersKey)).size()).isEqualTo(0);
            assertThat(((Map) mapValue.get(StatusInfoListener.messageListenersbyNameKey)).size()).isEqualTo(3);
            assertThat(((Map) mapValue.get(StatusInfoListener.messageListenersbyNameKey)).containsKey("test1")).isTrue();
            assertThat(((Map) mapValue.get(StatusInfoListener.messageListenersbyNameKey)).containsKey("test2")).isTrue();
            assertThat(mapValue.containsKey(StatusInfoListener.messagingThreadpoolstatsKey)).isFalse();

        } else {
            throw new RuntimeException("WRONG SENDER " + m.getSender());
        }
    }

}
