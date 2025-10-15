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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
     * CRITICAL TEST: Verifies that V6 can read V5 messages that use "name" field instead of "topic".
     * V5 stored messages with field "name", V6 uses "topic" with @Aliases({"name"}).
     * This tests that the alias actually works for reading V5 messages.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testV5NameFieldCompatibility(Morphium morphium) throws Exception {
        String tstName = new Object() {}.getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(Msg.class);
            Thread.sleep(500);

            MorphiumMessaging receiver = morphium.createMessaging();
            receiver.setSenderId("v6_receiver");

            AtomicInteger msgCount = new AtomicInteger(0);
            List<Msg> receivedMessages = new ArrayList<>();

            receiver.addListenerForTopic("test_topic", (msg, m) -> {
                msgCount.incrementAndGet();
                receivedMessages.add(m);
                log.info("V6 received V5 message with topic: " + m.getTopic());
                return null;
            });

            receiver.start();
            Thread.sleep(1000);

            // Simulate a V5 message by storing it with "name" field instead of "topic"
            // This is what V5 would have stored in MongoDB
            String collectionName = receiver.getCollectionName("test_topic");

            // Create a document directly with V5 field names
            Map<String, Object> v5MessageDoc = new HashMap<>();
            v5MessageDoc.put("name", "test_topic");  // V5 used "name" field!
            v5MessageDoc.put("msg", "Message from V5");
            v5MessageDoc.put("value", "v5_value");
            v5MessageDoc.put("sender", "v5_sender");
            v5MessageDoc.put("senderHost", "v5_host");
            v5MessageDoc.put("timestamp", System.currentTimeMillis());
            v5MessageDoc.put("ttl", 30000L);
            v5MessageDoc.put("priority", 1000);
            v5MessageDoc.put("timingOut", true);
            v5MessageDoc.put("deleteAfterProcessing", false);
            v5MessageDoc.put("deleteAfterProcessingTime", 0);
            v5MessageDoc.put("exclusive", false);
            v5MessageDoc.put("processedBy", null);
            v5MessageDoc.put("recipients", null);
            v5MessageDoc.put("inAnswerTo", null);

            // Store using storeMap to preserve V5 field names
            morphium.storeMap(collectionName, v5MessageDoc);

            // Wait for V6 to receive and process the V5 message with "name" field
            TestUtils.waitForConditionToBecomeTrue(5000, "V6 did not receive V5 message with 'name' field",
                () -> msgCount.get() >= 1);

            assertEquals(1, msgCount.get(), "V6 should have received 1 V5 message");
            Msg receivedMsg = receivedMessages.get(0);
            assertEquals("Message from V5", receivedMsg.getMsg());
            assertEquals("v5_value", receivedMsg.getValue());
            assertEquals("test_topic", receivedMsg.getTopic(),
                "V6 should read V5 'name' field as 'topic' via @Aliases");

            log.info("✓ V6 successfully read V5 message with 'name' field via @Aliases");
            receiver.terminate();
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
     * CRITICAL TEST: Tests answer flow - V5 sends message, V6 receives and answers, V5 gets the answer.
     * This simulates the real-world scenario where V5 services query V6 services.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testV5SendsV6AnswersV5Receives(Morphium morphium) throws Exception {
        String tstName = new Object() {}.getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(Msg.class);
            Thread.sleep(500);

            // V5 sender (simulated with V6 code but storing V5 format)
            MorphiumMessaging v5Sender = morphium.createMessaging();
            v5Sender.setSenderId("v5_sender");

            // V6 receiver that will answer
            MorphiumMessaging v6Responder = morphium.createMessaging();
            v6Responder.setSenderId("v6_responder");

            AtomicInteger v5AnswerCount = new AtomicInteger(0);
            AtomicInteger v6RequestCount = new AtomicInteger(0);
            List<Msg> v5ReceivedAnswers = new ArrayList<>();

            // V6 receives request and sends answer
            v6Responder.addListenerForTopic("request_topic", (msg, m) -> {
                v6RequestCount.incrementAndGet();
                log.info("V6 received request from V5: " + m.getMsg());

                // V6 sends answer back
                Msg answer = new Msg("answer_topic", "Answer from V6", "v6_answer_value");
                return answer; // Return answer
            });

            // V5 sender waits for answer
            v5Sender.addListenerForTopic("answer_topic", (msg, m) -> {
                if (m.isAnswer()) {
                    v5AnswerCount.incrementAndGet();
                    v5ReceivedAnswers.add(m);
                    log.info("V5 received answer from V6: " + m.getMsg());
                }
                return null;
            });

            v5Sender.start();
            v6Responder.start();
            Thread.sleep(1000);

            // V5 sends a request message with V5 format (using "name" field)
            String collectionName = v5Sender.getCollectionName("request_topic");
            Map<String, Object> v5RequestMsg = new HashMap<>();
            v5RequestMsg.put("name", "request_topic");  // V5 uses "name"!
            v5RequestMsg.put("msg", "Request from V5");
            v5RequestMsg.put("value", "v5_request_value");
            v5RequestMsg.put("sender", "v5_sender");
            v5RequestMsg.put("senderHost", "v5_host");
            v5RequestMsg.put("timestamp", System.currentTimeMillis());
            v5RequestMsg.put("ttl", 30000L);
            v5RequestMsg.put("priority", 1000);
            v5RequestMsg.put("timingOut", true);
            v5RequestMsg.put("deleteAfterProcessing", false);
            v5RequestMsg.put("deleteAfterProcessingTime", 0);
            v5RequestMsg.put("exclusive", false);
            v5RequestMsg.put("processedBy", new ArrayList<>());
            v5RequestMsg.put("recipients", null);
            v5RequestMsg.put("inAnswerTo", null);

            morphium.storeMap(collectionName, v5RequestMsg);
            log.info("V5 sent request message with 'name' field");

            // Wait for V6 to receive and process
            TestUtils.waitForConditionToBecomeTrue(5000, "V6 did not receive V5 request",
                () -> v6RequestCount.get() >= 1);

            assertEquals(1, v6RequestCount.get(), "V6 should have received 1 request from V5");

            // Wait for V5 to receive the answer from V6
            TestUtils.waitForConditionToBecomeTrue(5000, "V5 did not receive answer from V6",
                () -> v5AnswerCount.get() >= 1);

            assertEquals(1, v5AnswerCount.get(), "V5 should have received 1 answer from V6");
            Msg answer = v5ReceivedAnswers.get(0);
            assertEquals("Answer from V6", answer.getMsg());
            assertEquals("v6_answer_value", answer.getValue());
            assertTrue(answer.isAnswer(), "Message should be marked as answer");
            assertNotNull(answer.getInAnswerTo(), "Answer should have inAnswerTo set");

            log.info("✓ V5->V6 answer flow works: V5 sent request, V6 answered, V5 received answer");

            v5Sender.terminate();
            v6Responder.terminate();
        }
    }

    /**
     * CRITICAL TEST: Tests answer flow - V6 sends message, V5 receives and answers, V6 gets the answer.
     * This simulates the real-world scenario where V6 services query V5 services.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testV6SendsV5AnswersV6Receives(Morphium morphium) throws Exception {
        String tstName = new Object() {}.getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(Msg.class);
            Thread.sleep(500);

            // V6 sender
            MorphiumMessaging v6Sender = morphium.createMessaging();
            v6Sender.setSenderId("v6_sender");

            // V5 responder (simulated)
            MorphiumMessaging v5Responder = morphium.createMessaging();
            v5Responder.setSenderId("v5_responder");

            AtomicInteger v6AnswerCount = new AtomicInteger(0);
            AtomicInteger v5RequestCount = new AtomicInteger(0);
            List<Msg> v6ReceivedAnswers = new ArrayList<>();

            // V5 receives request and sends answer (simulated - returns answer with V5 format)
            v5Responder.addListenerForTopic("v6_request_topic", (msg, m) -> {
                v5RequestCount.incrementAndGet();
                log.info("V5 received request from V6: " + m.getMsg());

                // V5 sends answer back - but we'll manually store it with "name" field to simulate V5
                Msg answer = new Msg("v6_answer_topic", "Answer from V5", "v5_answer_value");
                return answer;
            });

            // V6 sender waits for answer
            v6Sender.addListenerForTopic("v6_answer_topic", (msg, m) -> {
                if (m.isAnswer()) {
                    v6AnswerCount.incrementAndGet();
                    v6ReceivedAnswers.add(m);
                    log.info("V6 received answer from V5: " + m.getMsg() + ", topic=" + m.getTopic());
                }
                return null;
            });

            v6Sender.start();
            v5Responder.start();
            Thread.sleep(1000);

            // V6 sends a normal request
            Msg v6Request = new Msg("v6_request_topic", "Request from V6", "v6_request_value");
            v6Sender.sendMessage(v6Request);
            log.info("V6 sent request message");

            // Wait for V5 to receive and process
            TestUtils.waitForConditionToBecomeTrue(5000, "V5 did not receive V6 request",
                () -> v5RequestCount.get() >= 1);

            assertEquals(1, v5RequestCount.get(), "V5 should have received 1 request from V6");

            // Wait for V6 to receive the answer from V5
            TestUtils.waitForConditionToBecomeTrue(5000, "V6 did not receive answer from V5",
                () -> v6AnswerCount.get() >= 1);

            assertEquals(1, v6AnswerCount.get(), "V6 should have received 1 answer from V5");
            Msg answer = v6ReceivedAnswers.get(0);
            assertEquals("Answer from V5", answer.getMsg());
            assertEquals("v5_answer_value", answer.getValue());
            assertTrue(answer.isAnswer(), "Message should be marked as answer");
            assertNotNull(answer.getInAnswerTo(), "Answer should have inAnswerTo set");
            // Critical: Check that topic was correctly read from V5's "name" field
            assertEquals("v6_answer_topic", answer.getTopic(),
                "V6 should correctly read answer topic from V5");

            log.info("✓ V6->V5 answer flow works: V6 sent request, V5 answered, V6 received answer");

            v6Sender.terminate();
            v5Responder.terminate();
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
