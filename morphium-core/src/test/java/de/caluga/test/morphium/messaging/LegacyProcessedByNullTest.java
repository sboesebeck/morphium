package de.caluga.test.morphium.messaging;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.messaging.DualChannelMessaging;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.messaging.MultiCollectionMessaging;
import de.caluga.morphium.messaging.SingleCollectionMessaging;
import de.caluga.test.mongo.suite.base.TestUtils;
import de.caluga.test.mongo.suite.inmem.MorphiumInMemTestBase;

/**
 * Issue #291: a stored message document with an explicit {@code processed_by: null} must still be
 * deliverable. Morphium senders initialize the field via Msg's {@code @PreStore}, but
 * legacy/foreign writers (other applications mapping the same collection without the guard, raw
 * driver writers, restored dumps) produce explicit nulls - and mongod rejects the
 * {@code $addToSet} marking on such a field ("non-array type null"). Since 6.3.x exclusive
 * messages MUST be marked before the listener runs, that turned the failed mark into a hard
 * non-delivery: no listener call, no answer, sender timeout.
 *
 * <p>The InMemoryDriver mirrors mongod's null/missing distinction since #291, so this reproduces
 * in-memory.
 */
@Tag("messaging")
@Tag("inmemory")
public class LegacyProcessedByNullTest extends MorphiumInMemTestBase {

    private static final String TOPIC = "legacynull";

    static Stream<String> implementations() {
        return Stream.of(SingleCollectionMessaging.NAME, DualChannelMessaging.NAME, MultiCollectionMessaging.NAME);
    }

    @ParameterizedTest
    @MethodSource("implementations")
    public void exclusiveMessageWithNullProcessedByIsStillDelivered(String impl) throws Exception {
        MorphiumMessaging consumer;
        switch (impl) {
            case SingleCollectionMessaging.NAME: consumer = new SingleCollectionMessaging(); break;
            case DualChannelMessaging.NAME: consumer = new DualChannelMessaging(); break;
            default: consumer = new MultiCollectionMessaging(); break;
        }
        consumer.init(morphium);
        CountDownLatch processed = new CountDownLatch(1);
        consumer.addListenerForTopic(TOPIC, (m, msg) -> {
            processed.countDown();
            return null;
        });

        // The legacy/foreign document: serialized WITHOUT Msg's @PreStore lifecycle (exactly how
        // a foreign entity or raw-driver writer stores it), carrying an explicit null.
        Msg legacy = new Msg(TOPIC, "legacy", "value", 120000, true);
        legacy.setMsgId(new MorphiumId());
        legacy.setSender("legacy-foreign-sender");
        legacy.setTimestamp(System.currentTimeMillis());
        Map<String, Object> doc = morphium.getMapper().serialize(legacy);
        doc.put("processed_by", null);
        String coll = consumer.getCollectionName(TOPIC);
        ((InMemoryDriver) morphium.getDriver()).insert(morphium.getDatabase(), coll, List.of(doc), null);

        try {
            consumer.start();
            assertTrue(consumer.waitForReady(30, TimeUnit.SECONDS), "consumer not ready");

            assertTrue(processed.await(15, TimeUnit.SECONDS),
                    "exclusive message with legacy processed_by:null must still reach the listener");

            // the mark must have repaired the field: null -> array containing the consumer
            TestUtils.waitForConditionToBecomeTrue(5000, "processed_by not repaired to an array with the consumer id",
                    () -> {
                        try {
                            List<Map<String, Object>> found = ((InMemoryDriver) morphium.getDriver())
                                    .find(morphium.getDatabase(), coll, Map.of("_id", legacy.getMsgId()), null, null, 0, 0);
                            if (found.size() != 1) {
                                return false;
                            }
                            Object pb = found.get(0).get("processed_by");
                            return pb instanceof List && ((List<?>) pb).contains(consumer.getSenderId());
                        } catch (Exception e) {
                            return false;
                        }
                    });
        } finally {
            consumer.terminate();
        }
    }
}
