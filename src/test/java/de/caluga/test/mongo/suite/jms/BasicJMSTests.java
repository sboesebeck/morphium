package de.caluga.test.mongo.suite.jms;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.jms.JMSConnectionFactory;
import de.caluga.morphium.messaging.jms.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import javax.jms.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import de.caluga.test.mongo.suite.base.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

@Tag("jms")
@Tag("slow")  // May be flaky under high parallel load - uses messaging with multiple contexts
public class BasicJMSTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void basicSendReceiveTest(Morphium morphium) throws Exception  {
        JMSConnectionFactory factory = new JMSConnectionFactory(morphium);
        JMSContext ctx1 = factory.createContext();
        JMSContext ctx2 = factory.createContext();

        JMSProducer pr1 = ctx1.createProducer();
        Topic dest = new JMSTopic("test1");

        JMSConsumer con = ctx2.createConsumer(dest);
        con.setMessageListener(message -> {
            log.info("Got Message!");
            if (message instanceof JMSTextMessage) {
                log.info("Got Text Message");
            } else if (message instanceof JMSMapMessage) {
                log.info("... Map Message");
            } else if (message instanceof JMSObjectMessage) {
                log.info("... got Object message");
            } else if (message instanceof JMSBytesMessage) {
                log.info("Got byte message");
                try {
                    ((JMSBytesMessage) message).readBoolean();
                    ((JMSBytesMessage) message).readInt();
                } catch (JMSException e) {
                    e.printStackTrace();
                }

            }
        });
        Thread.sleep(2000);  // Give more time for messaging to stabilize under parallel load
        pr1.send(dest, "A test");
        JMSTextMessage m = new JMSTextMessage();
        m.setText("test");
        pr1.send(dest, m);

        JMSMapMessage mm = new JMSMapMessage();
        mm.setDouble("dbl", 1.3);
        pr1.send(dest, mm);

        JMSBytesMessage mb = new JMSBytesMessage();
        mb.writeBoolean(true);
        mb.writeInt(1);
        mb.writeChar('a');
        mb.writeFloat(1.4f);
        mb.writeDouble(1.4d);
        mb.writeLong(123L);
        mb.writeShort((short) 12);
        mb.writeUTF("Hello");


        JMSObjectMessage mo = new JMSObjectMessage();
        mo.setObject("an object");
        pr1.send(dest, mo);
        ctx1.close();
        ctx2.close();
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void synchronousSendReceiveTest(Morphium morphium) throws Exception  {
        JMSConnectionFactory factory = new JMSConnectionFactory(morphium);
        JMSContext ctx1 = factory.createContext();
        JMSContext ctx2 = factory.createContext();

        JMSProducer pr1 = ctx1.createProducer();
        Topic dest = new JMSTopic("test1");
        JMSConsumer con = ctx2.createConsumer(dest);

        final Map<String, Object> exchange = new ConcurrentHashMap<>();
        Thread senderThread = new Thread(() -> {
            JMSTextMessage message = new JMSTextMessage();
            try {
                message.setText("Test");
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }

            pr1.send(dest, message);
            log.info("Sent out message");
            exchange.put("sent", true);
        });

        Thread receiverThread = new Thread(() -> {
            log.info("Receiving...");
            // We expect a message to arrive shortly, but on slower/loaded setups it can take a bit.
            // Use a bounded timeout to avoid hanging forever.
            Message msg = con.receive(10_000);
            log.info("Got incoming message: {}", msg);
            if (msg != null) {
                exchange.put("received", true);
            }
        });

        receiverThread.start();
        senderThread.start();

        // Deterministic: wait up to 15s for both flags, then fail.
        TestUtils.waitForConditionToBecomeTrue(15_000, "sender did not send", () -> exchange.get("sent") != null);
        TestUtils.waitForConditionToBecomeTrue(15_000, "receiver did not receive", () -> exchange.get("received") != null);

        // Join threads to make sure nothing is still running when we close contexts.
        senderThread.join(5_000);
        receiverThread.join(5_000);

        assertNotNull(exchange.get("sent"));
        assertNotNull(exchange.get("received"));

        ctx1.close();
        ctx2.close();
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void consumerProducerTest(Morphium morphium) throws Exception  {
        MorphiumMessaging m = morphium.createMessaging();
        Consumer consumer = new Consumer(m, new JMSTopic("jmstopic_test"));
        m.start();
        MorphiumMessaging m2 = morphium.createMessaging();
        Producer producer = new Producer(m2);

        producer.send(new JMSTopic("jmstopic_test"), "This is the body");
        Message msg = consumer.receive();

        assertNotNull(msg);
        ;

        m.terminate();
        m2.terminate();

        consumer.close();


    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void consumerProducerQueueTest(Morphium morphium) throws Exception  {
        MorphiumMessaging m = morphium.createMessaging();
        Consumer consumer = new Consumer(m, new JMSQueue());
        m.start();
        MorphiumMessaging m2 = morphium.createMessaging();
        Producer producer = new Producer(m2);

        MorphiumMessaging m3 = morphium.createMessaging();
        Consumer consumer2 = new Consumer(m3, new JMSQueue());

        producer.send(new JMSQueue(), "This is the body");
        Message msg = consumer.receive(1000);
        Message msg2 = consumer2.receive(1000);
        assertTrue(msg != null || msg2 != null);
        ;
        assert (msg != msg2);

        m.terminate();
        m2.terminate();
        m3.terminate();

        consumer.close();
        consumer2.close();


    }

}
