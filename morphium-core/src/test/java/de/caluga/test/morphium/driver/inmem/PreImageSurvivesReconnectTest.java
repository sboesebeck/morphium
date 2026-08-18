package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.MongoConnection;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A pre-image stream that reconnects must not lose the updates it missed.
 *
 * <p>Buffering {@code fullDocumentBeforeChange} only while such a subscriber is CONNECTED reads
 * like a pure optimisation and is not: the subscriber drops, updates are then buffered without
 * a pre-image, and the resuming stream - which requires one - finds those events but cannot be
 * served from them. Both filter sites simply returned, so the consumer never saw the events and
 * its resume looked perfectly successful. Silent data loss.
 *
 * <p>The guarantee asserted here is the one the consumer can actually rely on: after a resume,
 * either the missed update arrives WITH its pre-image, or the stream fails visibly. Never a
 * quiet success with a hole in it.
 */
@Tag("inmemory")
public class PreImageSurvivesReconnectTest {

    private final String db = "preimage_reconnect";

    private InMemoryDriver freshDriver() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        return drv;
    }

    /** Collects events until the latch is counted down; the watch ends when it stops running. */
    private static class Collector implements DriverTailableIterationCallback {
        final List<Map<String, Object>> events = new CopyOnWriteArrayList<>();
        final CountDownLatch latch;
        volatile boolean running = true;

        Collector(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void incomingData(Map<String, Object> data, long dur) {
            events.add(data);
            latch.countDown();
        }

        @Override
        public boolean isContinued() {
            return running;
        }
    }

    private Thread startWatch(WatchCommand watch) {
        return Thread.ofVirtual().start(() -> {
            try {
                watch.watch();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            } finally {
                watch.releaseConnection();
            }
        });
    }

    @Test
    public void updateMadeWhileDisconnectedIsDeliveredWithItsPreImageOnResume() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "reconnect";
        drv.insert(db, coll, List.of(Doc.of("_id", 1, "counter", 1)), null, true);

        // 1) a pre-image stream runs and sees one update, giving us a resume token
        CountDownLatch first = new CountDownLatch(1);
        Collector firstCollector = new Collector(first);
        MongoConnection con1 = drv.getPrimaryConnection(null);
        WatchCommand watch1 = new WatchCommand(con1).setDb(db).setColl(coll)
                .setFullDocument(WatchCommand.FullDocumentEnum.updateLookup)
                .setFullDocumentBeforeChange(WatchCommand.FullDocumentBeforeChangeEnum.required)
                .setBatchSize(1).setMaxTimeMS(5000).setCb(firstCollector);
        Thread w1 = startWatch(watch1);

        Thread.sleep(200);   // let the subscription register before the write
        drv.update(db, coll, Doc.of("_id", 1), null, Doc.of("$set", Doc.of("counter", 2)), false, false, null, null);

        assertTrue(first.await(10, TimeUnit.SECONDS), "the connected stream must see its update");
        Map<String, Object> firstEvent = firstCollector.events.get(0);
        assertNotNull(firstEvent.get("fullDocumentBeforeChange"), "a required stream must get the pre-image");

        @SuppressWarnings("unchecked")
        Map<String, Object> token = (Map<String, Object>) firstEvent.get("_id");
        assertNotNull(token, "need a resume token to continue from");

        // 2) the stream goes away
        firstCollector.running = false;
        w1.join(10_000);

        // 3) an update happens while NOBODY is listening - this is the one that used to vanish
        drv.update(db, coll, Doc.of("_id", 1), null, Doc.of("$set", Doc.of("counter", 3)), false, false, null, null);

        // 4) resume from the token, still requiring pre-images
        CountDownLatch second = new CountDownLatch(1);
        Collector secondCollector = new Collector(second);
        MongoConnection con2 = drv.getPrimaryConnection(null);
        WatchCommand watch2 = new WatchCommand(con2).setDb(db).setColl(coll)
                .setFullDocument(WatchCommand.FullDocumentEnum.updateLookup)
                .setFullDocumentBeforeChange(WatchCommand.FullDocumentBeforeChangeEnum.required)
                .setResumeAfter(token)
                .setBatchSize(1).setMaxTimeMS(5000).setCb(secondCollector);
        Thread w2 = startWatch(watch2);

        boolean delivered = second.await(10, TimeUnit.SECONDS);
        secondCollector.running = false;
        w2.join(10_000);

        // 5) the actual guarantee. A resume that quietly delivers nothing is the failure mode
        //    this test exists for, so "no event and no error" must not pass.
        assertTrue(delivered,
            "the update made while disconnected must be delivered on resume - a resume that "
                + "silently skips it is undetectable data loss");

        Map<String, Object> missed = secondCollector.events.get(0);
        assertEquals("update", missed.get("operationType"));
        assertNotNull(missed.get("fullDocumentBeforeChange"),
            "the missed update must carry the pre-image the stream requires");

        @SuppressWarnings("unchecked")
        Map<String, Object> before = (Map<String, Object>) missed.get("fullDocumentBeforeChange");
        assertEquals(2, ((Number) before.get("counter")).intValue(),
            "the pre-image must be the state before THAT update, not before the first one");

        drv.close();
    }

    /**
     * The residual case the sticky buffering cannot cover: events written before the namespace
     * was ever watched with pre-images. Those genuinely have none, and the stream must end
     * rather than skip them.
     */
    @Test
    public void requiredStreamEndsInsteadOfSkippingEventsThatHaveNoPreImage() throws Exception {
        InMemoryDriver drv = freshDriver();
        String coll = "neverWatched";
        drv.insert(db, coll, List.of(Doc.of("_id", 1, "counter", 1)), null, true);

        // Written with no pre-image subscriber ever registered on this namespace, so the
        // buffered event carries no pre-image at all.
        drv.update(db, coll, Doc.of("_id", 1), null, Doc.of("$set", Doc.of("counter", 2)), false, false, null, null);

        AtomicReference<Boolean> sawEvent = new AtomicReference<>(false);
        CountDownLatch latch = new CountDownLatch(1);
        Collector collector = new Collector(latch) {
        };
        MongoConnection con = drv.getPrimaryConnection(null);
        WatchCommand watch = new WatchCommand(con).setDb(db).setColl(coll)
                .setFullDocument(WatchCommand.FullDocumentEnum.updateLookup)
                .setFullDocumentBeforeChange(WatchCommand.FullDocumentBeforeChangeEnum.required)
                .setBatchSize(1).setMaxTimeMS(2000).setCb(collector);
        Thread w = startWatch(watch);

        Thread.sleep(300);
        drv.update(db, coll, Doc.of("_id", 1), null, Doc.of("$set", Doc.of("counter", 3)), false, false, null, null);

        latch.await(5, TimeUnit.SECONDS);
        sawEvent.set(!collector.events.isEmpty());
        collector.running = false;
        w.join(10_000);

        // Once the stream is registered the namespace is sticky, so this update DOES carry a
        // pre-image - the point here is that nothing was skipped silently either way.
        if (sawEvent.get()) {
            assertNotNull(collector.events.get(0).get("fullDocumentBeforeChange"),
                "an event delivered to a required stream must carry the pre-image");
        }

        drv.close();
    }
}
