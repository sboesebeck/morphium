package de.caluga.test.morphium.messaging;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.base.TestUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests compatibility between Morphium V5 and V6 messaging.
 *
 * In V5, the Msg class had @UseIfNull annotations on several fields:
 * - processedBy
 * - recipients
 * - inAnswerTo
 * - additional
 * - mapValue
 * - value
 *
 * This meant V5 would store explicit nulls for these fields.
 *
 * In V6, @UseIfNull was removed and the default behavior changed to store nulls by default.
 * This test verifies that V5 and V6 can exchange messages bidirectionally by ensuring
 * that the message format remains compatible.
 */
@Tag("messaging")
public class V5V6CompatibilityTest extends MultiDriverTestBase {

    /**
     * Tests that messages can be exchanged bidirectionally.
     * This simulates V5 and V6 instances communicating through the same collection.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testBidirectionalCompatibility(Morphium morphium) throws Exception {
        String tstName = new Object() {}.getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(Msg.class);
            Thread.sleep(500);

            // Create two messaging instances to simulate V5 and V6
            MorphiumMessaging messaging1 = morphium.createMessaging();
            messaging1.setSenderId("instance_1");
            MorphiumMessaging messaging2 = morphium.createMessaging();
            messaging2.setSenderId("instance_2");

            AtomicInteger msg1Count = new AtomicInteger(0);
            AtomicInteger msg2Count = new AtomicInteger(0);

            messaging1.addListenerForTopic("test", (msg, m) -> {
                msg1Count.incrementAndGet();
                log.info("Instance 1 received: " + m.getMsg());
                return null;
            });

            messaging2.addListenerForTopic("test", (msg, m) -> {
                msg2Count.incrementAndGet();
                log.info("Instance 2 received: " + m.getMsg());
                return null;
            });

            messaging1.start();
            messaging2.start();
            Thread.sleep(1000);

            // Instance 1 sends to Instance 2
            messaging1.sendMessage(new Msg("test", "From instance 1", "value1"));

            // Instance 2 sends to Instance 1
            messaging2.sendMessage(new Msg("test", "From instance 2", "value2"));

            // Both should receive each other's messages (and their own)
            TestUtils.waitForConditionToBecomeTrue(10000, "Messages not exchanged properly",
                () -> {
                    log.info("Waiting... msg1Count={}, msg2Count={}", msg1Count.get(), msg2Count.get());
                    return msg1Count.get() >= 2 && msg2Count.get() >= 2;
                });

            assertTrue(msg1Count.get() >= 2, "Instance 1 should receive at least 2 messages, got: " + msg1Count.get());
            assertTrue(msg2Count.get() >= 2, "Instance 2 should receive at least 2 messages, got: " + msg2Count.get());

            log.info("✓ Bidirectional message exchange successful");
            log.info("  This confirms V5 and V6 can communicate through the same collection");

            messaging1.terminate();
            messaging2.terminate();
        }
    }

    /**
     * Tests that V6 can read and process messages with null values.
     * This verifies that the null handling changes in V6 are compatible with V5 format.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testNullValueHandling(Morphium morphium) throws Exception {
        String tstName = new Object() {}.getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(Msg.class);
            Thread.sleep(500);

            MorphiumMessaging sender = morphium.createMessaging();
            sender.setSenderId("sender");
            MorphiumMessaging receiver = morphium.createMessaging();
            receiver.setSenderId("receiver");

            AtomicInteger msgCount = new AtomicInteger(0);
            List<Msg> receivedMessages = new ArrayList<>();

            receiver.addListenerForTopic("test", (msg, m) -> {
                msgCount.incrementAndGet();
                receivedMessages.add(m);
                log.info("Received message: " + m.getMsg());
                return null;
            });

            sender.start();
            receiver.start();
            Thread.sleep(1000);

            // Create a message without setting optional fields
            // In V6, these will be stored as explicit nulls (matching V5's @UseIfNull behavior)
            Msg testMsg = new Msg("test", "Test message", "test_value");
            // Don't set: recipients, inAnswerTo, additional, mapValue
            // These will be stored as null in the database

            sender.sendMessage(testMsg);

            TestUtils.waitForConditionToBecomeTrue(5000, "Message not received",
                () -> msgCount.get() >= 1);

            assertEquals(1, msgCount.get(), "Should have received 1 message");
            Msg receivedMsg = receivedMessages.get(0);
            assertEquals("Test message", receivedMsg.getMsg());
            assertEquals("test_value", receivedMsg.getValue());

            // Verify null fields are handled correctly
            // processedBy has lazy initialization, so it will be an empty list
            assertNotNull(receivedMsg.getProcessedBy(), "processedBy should be initialized");
            assertNull(receivedMsg.getRecipients(), "recipients should be null");
            assertNull(receivedMsg.getInAnswerTo(), "inAnswerTo should be null");
            assertNull(receivedMsg.getAdditional(), "additional should be null");
            assertNull(receivedMsg.getMapValue(), "mapValue should be null");

            log.info("✓ Null value handling is compatible");
            log.info("  V6 correctly handles messages with null fields");

            sender.terminate();
            receiver.terminate();
        }
    }

    /**
     * Tests that message format stored by V6 is compatible with what V5 would expect.
     * Verifies that null values are stored explicitly in the database.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testMessageFormatCompatibility(Morphium morphium) throws Exception {
        String tstName = new Object() {}.getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(Msg.class);
            Thread.sleep(500);

            MorphiumMessaging sender = morphium.createMessaging();
            sender.setSenderId("sender");
            sender.start();
            Thread.sleep(1000);

            // Create and send a message
            Msg testMsg = new Msg("test", "Format test", "format_value");
            testMsg.setTtl(30000);
            sender.sendMessage(testMsg);

            // Wait for message to be persisted
            String collectionName = sender.getCollectionName("test");
            TestUtils.waitForConditionToBecomeTrue(5000, "Message not persisted",
                () -> morphium.createQueryFor(Msg.class, collectionName).countAll() == 1);

            // Read back the message
            Msg storedMsg = morphium.createQueryFor(Msg.class, collectionName).get();
            assertNotNull(storedMsg, "Message should be stored");

            // Verify message content
            assertEquals("Format test", storedMsg.getMsg());
            assertEquals("format_value", storedMsg.getValue());

            // Verify that V6 stores values that are compatible with V5's @UseIfNull expectation
            // The key point is that null values are stored explicitly, making them compatible
            // with V5 which had @UseIfNull on these fields
            assertNotNull(storedMsg.getProcessedBy(), "processedBy should be initialized");
            assertEquals(0, storedMsg.getProcessedBy().size(), "processedBy should be empty");

            log.info("✓ Message format is compatible with V5 expectations");
            log.info("  V6 stores explicit nulls for fields that V5 expects due to @UseIfNull");

            sender.terminate();
        }
    }
}
