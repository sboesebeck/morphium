package de.caluga.test.mongo.suite.jms;

import de.caluga.morphium.messaging.jms.JMSConnectionFactory;
import de.caluga.morphium.messaging.jms.*;
import de.caluga.test.mongo.suite.MorphiumTestBase;
import org.junit.Test;

import javax.jms.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BasicJMSTests extends MorphiumTestBase {

    @Test
    public void basicSendReceiveTest() throws Exception {
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
        Thread.sleep(1000);
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
