package de.caluga.test.mongo.suite.jms;

import de.caluga.morphium.messaging.jms.JMSConnectionFactory;
import de.caluga.morphium.messaging.jms.JMSTopic;
import de.caluga.test.mongo.suite.MongoTest;
import org.junit.Test;

import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Topic;

public class BasicJMSTests extends MongoTest {

    @Test
    public void basicSendREceiveTest() throws Exception {
        JMSConnectionFactory factory = new JMSConnectionFactory(morphium);
        JMSContext ctx1 = factory.createContext();
        JMSContext ctx2 = factory.createContext();

        JMSProducer pr1 = ctx1.createProducer();
        Topic dest = new JMSTopic("test1");

        JMSConsumer con = ctx2.createConsumer(dest);
        con.setMessageListener(message -> log.info("Got Message!"));
        pr1.send(dest, "A test");

        Thread.sleep(1000);
    }
}
