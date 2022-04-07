package de.caluga.test.mongo.suite.messaging;

import com.rabbitmq.client.*;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.Test;

import java.io.IOException;

public class NoListenerTest extends MorphiumTestBase {

    @Test
    public void removeListenerTest() throws Exception {
        Messaging sender = new Messaging(morphium, 10, false);
        sender.start();

        Messaging r1 = new Messaging(morphium, 10, false);
        r1.start();

        MessageListener ml = (msg, m) -> {
            log.info("Got Message");
            return null;
        };

        r1.addListenerForMessageNamed("test_listener", ml);

        Msg m = new Msg("test_listener", "a test", "42");
        sender.sendMessage(m);
        Thread.sleep(1000);
        morphium.reread(m);

        assert (m.getProcessedBy().size() == 1);
        r1.removeListenerForMessageNamed("test_listener", ml);
        m = new Msg("test_listener", "a test", "42");
        sender.sendMessage(m);
        morphium.reread(m);
        assert (m.getProcessedBy() == null || m.getProcessedBy().size() == 0);

    }

    @Test
    public void rabbitMQTest() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
//        factory.setUri("amqp://userName:password@hostName:portNumber/virtualHost");
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");
        factory.setHost("localhost");
        factory.setPort(5672);

        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        String exchangeName = "msg_" + System.currentTimeMillis();
        String queueName = "queue";

        channel.exchangeDeclare(exchangeName, "fanout", true);
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, exchangeName, "route1");
        //channel.queuePurge(queueName);

        ///////// connection 2
        Connection conn2 = factory.newConnection();
        Channel channel2 = conn2.createChannel();

        channel2.exchangeDeclare(exchangeName, "fanout", true);
        //AMQP.Exchange.DeclareOk ret = channel2.exchangeDeclarePassive(exchangeName);
//        channel2.queueDeclarePassive(queueName);
        channel2.queueDeclare(queueName + "2", true, false, false, null);
        channel2.queueBind(queueName + "2", exchangeName, "route1");
        // channel.queueBind(queueName, exchangeName, "route1");


        log.info("Starting listener");
        boolean autoAck = false;
        channel.basicConsume(queueName + "2", autoAck,
                new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body)
                            throws IOException {
                        String routingKey = envelope.getRoutingKey();
                        String contentType = properties.getContentType();
                        long deliveryTag = envelope.getDeliveryTag();

                        log.info("1st Consumer: Incoming Message! " + properties.getMessageId() + " msg: " + new String(body));
                        // (process the message components here ...)
                        channel.basicAck(deliveryTag, false);
                    }
                });

        channel2.basicConsume(queueName, autoAck,
                new DefaultConsumer(channel2) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body)
                            throws IOException {
                        String routingKey = envelope.getRoutingKey();
                        String contentType = properties.getContentType();
                        long deliveryTag = envelope.getDeliveryTag();
                        // (process the message components here ...)
                        log.info("2nd consumer: Incoming Message! " + properties.getMessageId() + " msg: " + new String(body));
                        channel2.basicAck(deliveryTag, false);
                    }
                });

        log.info("done");
        for (int i = 0; i < 10; i++) {
            byte[] messageBodyBytes = ("Hello, world! " + i).getBytes();
            channel.basicPublish(exchangeName, "route1",
                    new AMQP.BasicProperties.Builder()
                            .contentType("text/plain")
                            .deliveryMode(2)
                            .messageId("Sent_" + i)
                            .priority(1)
                            .userId("guest")
                            .build(),
                    messageBodyBytes);
            Thread.sleep(100);
            messageBodyBytes = ("Hello, from chan2! " + i).getBytes();
            channel2.basicPublish(exchangeName, "route1",
                    new AMQP.BasicProperties.Builder()
                            .contentType("text/plain")
                            .deliveryMode(2)
                            .messageId("Sent_" + i)
                            .priority(1)
                            .userId("guest")
                            .build(),
                    messageBodyBytes);
            Thread.sleep(100);

        }

        Thread.sleep(10000);

    }
}
