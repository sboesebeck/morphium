package de.caluga.test.mongo.suite.jms;

import de.caluga.morphium.messaging.jms.JMSConnectionFactory;
import de.caluga.morphium.messaging.jms.JMSTextMessage;
import de.caluga.morphium.messaging.jms.JMSTopic;
import de.caluga.test.mongo.suite.MongoTest;
import org.junit.Test;

import javax.jms.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BasicJMSTests extends MongoTest {

    @Test
    public void basicSendReceiveTest() throws Exception {
        JMSConnectionFactory factory = new JMSConnectionFactory(morphium);
        JMSContext ctx1 = factory.createContext();
        JMSContext ctx2 = factory.createContext();

        JMSProducer pr1 = ctx1.createProducer();
        Topic dest = new JMSTopic("test1");

        JMSConsumer con = ctx2.createConsumer(dest);
        con.setMessageListener(message -> log.info("Got Message!"));
        pr1.send(dest, "A test");

        ctx1.close();
        ctx2.close();
    }

    @Test
    public void synchronousSendRecieveTest() throws Exception {
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
                e.printStackTrace();
            }
            pr1.send(dest, message);
            log.info("Sent out message");
            exchange.put("sent", true);
        });
        Thread receiverThread = new Thread(() -> {
            log.info("Receiving...");
            Message msg = con.receive();
            log.info("Got incoming message");
            exchange.put("received", true);
        });
        receiverThread.start();
        senderThread.start();
        Thread.sleep(5000);
        assert (exchange.get("sent") != null);
        assert (exchange.get("received") != null);
    }


}
