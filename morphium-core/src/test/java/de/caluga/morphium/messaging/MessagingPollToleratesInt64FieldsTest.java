package de.caluga.morphium.messaging;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.Doc;

/**
 * Regression test for the DriverFailoverProxyTest failure mode: the polling path of
 * {@code SingleCollectionMessaging.getMessagesForProcessing} cast {@code priority} hard to
 * {@code Integer} (and {@code timestamp} to {@code Long}). A message document whose numeric
 * fields arrive as the other boxed type - int64 over the wire, which real MongoDB may produce
 * at any time and which demonstrably occurs after a PoppyDB failover - killed EVERY poll with a
 * ClassCastException: the receiver silently never recovered ("received stayed at N"), because
 * the poll is exactly the path that picks up the backlog after a changestream outage.
 *
 * <p>The changestream path of the very same class has always handled this tolerantly
 * ({@code ((Number) prio).intValue()}, see its cs-event handler) - the classic two-paths drift.
 * The fix brings the poll path (in SingleCollectionMessaging AND DualChannelMessaging) to the
 * same rule.
 *
 * <p>Setup isolates the poll path: the message is planted (with int64 fields) BEFORE the
 * receiver starts, so no live changestream event exists for it - only polling can find it.
 */
@Tag("messaging")
public class MessagingPollToleratesInt64FieldsTest {

    private Morphium morphium;

    @BeforeEach
    void setup() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase("int64poll");
        cfg.driverSettings().setDriverName("InMemDriver");
        morphium = new Morphium(cfg);
    }

    @AfterEach
    void teardown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    @Test
    void backlogMessageWithInt64PriorityIsStillDelivered() throws Exception {
        // Plant a pending broadcast whose priority is a Long - written through a sender, then
        // rewritten raw so the stored field is int64, exactly what the failover run produced.
        MorphiumMessaging sender = morphium.createMessaging();
        sender.start();
        try {
            Msg m = new Msg("int64topic", "backlog", "value");
            m.setExclusive(false);
            sender.sendMessage(m);
            Thread.sleep(300);

            String coll = sender.getCollectionName();
            var drv = (de.caluga.morphium.driver.inmem.InMemoryDriver) morphium.getDriver();
            var found = drv.find("int64poll", coll, Doc.of(), null, null, 0, 10);
            assertTrue(!found.isEmpty(), "precondition: the planted message must be stored");

            for (var doc : found) {
                doc.put("priority", 100L);                       // int64, not int32
                drv.store("int64poll", coll, java.util.List.of(doc), null);
            }
        } finally {
            sender.terminate();
        }

        // Fresh receiver AFTER the plant: no live changestream event exists for the message,
        // so only the polling path can deliver it.
        MorphiumMessaging receiver = morphium.createMessaging();
        receiver.setPause(50);
        CountDownLatch got = new CountDownLatch(1);
        receiver.addListenerForTopic("int64topic", (mm, msg) -> {
            got.countDown();
            return null;
        });
        receiver.start();

        try {
            assertTrue(got.await(10, TimeUnit.SECONDS),
                    "the poll path must deliver a backlog message whose priority arrived as "
                            + "int64 - a hard (Integer) cast kills every poll and the receiver "
                            + "never recovers");
        } finally {
            receiver.terminate();
        }
    }
}
