package de.caluga.test.mongo.suite.messaging;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.rabbitmq.client.*;
import de.caluga.morphium.Utils;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.mongodb.Driver;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.writer.MorphiumWriter;
import de.caluga.test.mongo.suite.MorphiumTestBase;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.bson.Document;
import org.json.simple.parser.ParseException;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SpeedTests extends MorphiumTestBase {

    @Test
    public void writeSpeed() throws Exception {
        Messaging msg = new Messaging(morphium, 100, false, true, 10);
        msg.start();


        final long dur = 1000;

        final long start = System.currentTimeMillis();

        for (int i = 0; i < 25; i++) {
            new Thread() {
                public void run() {
                    Msg m = new Msg("test", "test", "testval", 30000);
                    while (System.currentTimeMillis() < start + dur) {
                        msg.sendMessage(m);
                        m.setMsgId(null);
                    }
                }
            }.start();
        }
        while (System.currentTimeMillis() < start + dur) {
            Thread.sleep(10);
        }
        long cnt = msg.getMessageCount();
        log.info("stored msg: " + cnt + " in " + dur + "ms");
        msg.terminate();
    }

    @Test
    public void writeRecSpeed() throws Exception {
//        morphium.getConfig().setThreadPoolAsyncOpCoreSize(1000);
        Messaging sender = new Messaging(morphium, 100, false, true, 10);
        sender.start();
        Messaging receiver = new Messaging(morphium, 100, true, true, 100);
        receiver.start();
        final AtomicInteger recCount = new AtomicInteger();

        receiver.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                recCount.incrementAndGet();
                return null;
            }
        });

        final long dur = 1000;

        final long start = System.currentTimeMillis();

        for (int i = 0; i < 15; i++) {
            new Thread() {
                public void run() {
                    Msg m = new Msg("test", "test", "testval", 30000);
                    while (System.currentTimeMillis() < start + dur) {
                        sender.sendMessage(m);
                        m.setMsgId(null);
                    }
                }
            }.start();
        }

        while (System.currentTimeMillis() < start + dur) {
            Thread.sleep(10);
        }
        long cnt = sender.getMessageCount();
        log.info("Messages sent: " + cnt + " received: " + recCount.get() + " in " + dur + "ms");
        sender.terminate();
        receiver.terminate();
    }

    @Test
    public void writeExclusiveRec() throws Exception {
//        morphium.getConfig().setThreadPoolAsyncOpCoreSize(1000);
        Messaging sender = new Messaging(morphium, 100, false, true, 10);
        sender.start();
        Messaging receiver = new Messaging(morphium, 100, true, true, 100);
        receiver.start();
        Messaging receiver2 = new Messaging(morphium, 100, true, true, 100);
        receiver2.start();
        final AtomicInteger recCount = new AtomicInteger();

        receiver.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                recCount.incrementAndGet();
                return null;
            }
        });
        receiver2.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                recCount.incrementAndGet();
                return null;
            }
        });

        final long dur = 1000;

        final long start = System.currentTimeMillis();

        for (int i = 0; i < 15; i++) {
            new Thread() {
                public void run() {
                    Msg m = new Msg("test", "test", "testval", 30000);
                    m.setExclusive(true);
                    while (System.currentTimeMillis() < start + dur) {
                        sender.sendMessage(m);
                        m.setMsgId(null);
                    }
                }
            }.start();
        }

        while (System.currentTimeMillis() < start + dur) {
            Thread.sleep(10);
        }
        long cnt = sender.getMessageCount();
        log.info("Messages sent: " + cnt + " received: " + recCount.get() + " in " + dur + "ms");
        sender.terminate();
        receiver.terminate();
    }


    @Test
    public void serializerSpeedTest() {
        UncachedObject o = new UncachedObject();
        o.setCounter(1);
        o.setValue("value");
        o.setLongData(new long[]{12L, 123L});
        o.setDval(12.3);

        int count = 0;
        long start = System.currentTimeMillis();

        while (System.currentTimeMillis() - start < 1000) {
            morphium.getMapper().serialize(o);
            count++;
        }
        log.info("Serialized " + count);
    }


    @Test
    public void writeSpeedTest() throws Exception {
        UncachedObject o = new UncachedObject();
        o.setCounter(1);
        o.setValue("value");
        o.setLongData(new long[]{12L, 123L});
        o.setDval(12.3);
        Map<String, Object> m = morphium.getMapper().serialize(o);
        List<Map<String, Object>> l = new ArrayList<>();
        l.add(m);
        int count = 0;
        long start = System.currentTimeMillis();
        Driver d = (Driver) morphium.getDriver();
        while (System.currentTimeMillis() - start < 1000) {
            MongoDatabase db = d.getDb(morphium.getConfig().getDatabase());
            MongoCollection<Document> c = d.getCollection(db, "uncached_object", ReadPreference.nearest(), WriteConcern.getWc(0, false, false, 100));
            c = c.withWriteConcern(com.mongodb.WriteConcern.UNACKNOWLEDGED);

            Document doc = new Document(m);
            c.insertOne(doc);
            count++;
        }
        log.info("wrote " + count);
    }


    @Test
    public void writeSpeedTestMorphium() throws Exception {
        Thread.sleep(2000);
        Msg o = new Msg();
        o.setName("test");
        o.setValue("value");
        o.setSender("tester");
        morphium.dropCollection(Msg.class);
        Thread.sleep(2000);

        morphium.getConfig().setAutoIndexAndCappedCreationOnWrite(false);
        morphium.store(o);
        o.setMsgId(null);
        final int pause = 1000;
        int count = 0;
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < pause) {
            morphium.store(o, "msg", null);
            o.setMsgId(null);
            count++;
        }
        log.info("wrote using morphium " + count);
        int mrph = count;
        morphium.dropCollection(Msg.class);
        count = 0;
        start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < pause) {
            morphium.getARHelper().isAnnotationPresentInHierarchy(o.getClass(), Entity.class);
            Map<String, Object> m = morphium.getMapper().serialize(o);
            List<Map<String, Object>> l = new ArrayList<>();
            l.add(m);
            morphium.getDriver().insert(morphium.getConfig().getDatabase(), morphium.getMapper().getCollectionName(o.getClass()), l, morphium.getWriteConcernForClass(o.getClass()));
//            writer.insert(o,"uncached_object",null);
            count++;
        }
        log.info("wrote using driver " + count);
        log.info("FActor: " + ((double) count / (double) mrph));

        morphium.dropCollection(Msg.class);
        count = 0;
        start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < pause) {
            morphium.getARHelper().isAnnotationPresentInHierarchy(o.getClass(), Entity.class);
            Map<String, Object> m = morphium.getMapper().serialize(o);
            List<Map<String, Object>> l = new ArrayList<>();
            l.add(m);
            MorphiumWriter writer = morphium.getWriterForClass(Msg.class);
//            morphium.getDriver().insert(morphium.getConfig().getDatabase(),morphium.getMapper().getCollectionName(o.getClass()),l,morphium.getWriteConcernForClass(o.getClass()));
            writer.insert(o, "msg", null);
            o.setMsgId(null);
            count++;
        }
        log.info("wrote using writer " + count);
        log.info("FActor: " + ((double) count / (double) mrph));

    }

    @Test
    public void rabbitMQTest() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
// "guest"/"guest" by default, limited to localhost connections
//        factory.setUsername(userName);
//        factory.setPassword(password);
//        factory.setVirtualHost(virtualHost);
        factory.setHost("localhost");
//        factory.setPort(portNumber);

        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        //channel.exchangeDeclare(exchangeName, "direct", true);
        String queueName = "Hello";
        //channel.queueBind(queueName, exchangeName, routingKey);
        channel.queueDeclare(queueName, false, false, false, null);

        //AMQP.Queue.DeclareOk response = channel.queueDeclarePassive("queue-name");
// returns the number of messages in Ready state in the queue
//        response.getMessageCount();
// returns the number of consumers the queue has
//        response.getConsumerCount();


        final AtomicInteger cnt = new AtomicInteger();

        DeliverCallback deliverCallback = new DeliverCallback() {
            @Override
            public void handle(String s, Delivery delivery) throws IOException {
                String message = new String(delivery.getBody(), "UTF-8");
//                System.out.println(" [x] Received '" + message + "'");
                try {
                    morphium.getMapper().deserialize(Msg.class, message);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                cnt.incrementAndGet();
            }
        };
        channel.basicConsume(queueName, true, deliverCallback, new CancelCallback() {
            @Override
            public void handle(String s) throws IOException {

            }
        });
        Thread.sleep(1000);
        long start = System.currentTimeMillis();
        long sent = 0;
        while (System.currentTimeMillis() - start < 1000) {
            Msg m = new Msg("test", "msg", "value");
            m.setMsgId(new MorphiumId());
            Map<String, Object> serialize = morphium.getMapper().serialize(m);
            String json = Utils.toJsonString(serialize);
            channel.basicPublish("", queueName, null, json.getBytes("utf-8"));
            sent++;
        }
        log.info("Sent: " + sent + " received " + cnt.get());
        //hannel.basicPublish("", queueName, null, messageBodyBytes);
        Thread.sleep(1000);


    }

    @Test
    public void testRabbitMqMessaging() throws Exception {
        Messaging receiver = new Messaging("localhost", morphium, "tst1", true);
        receiver.setSenderId("receiver");
        receiver.start();

        Messaging receiver2 = new Messaging("localhost", morphium, "tst1", true);
        receiver2.setSenderId("receiver2");
        receiver2.start();

        AtomicInteger rec = new AtomicInteger();
        AtomicInteger rec2 = new AtomicInteger();

        receiver.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
//                log.info("INCOMING MESSAGE!!!");
                rec.incrementAndGet();
                return null;
            }
        });
        receiver2.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
//                log.info("INCOMING MESSAGE!!!");
                rec2.incrementAndGet();
                return null;
            }
        });
        Messaging sender = new Messaging("localhost", morphium, "tst1", true);
        sender.setSenderId("sender");
        sender.start();
        sender.addMessageListener(new MessageListener() {
            @Override
            public Msg onMessage(Messaging msg, Msg m) throws InterruptedException {
                log.error("SHould not be here!!!! " + Utils.toJsonString(m));
                return null;
            }
        });

        long start = System.currentTimeMillis();
        int count = 0;
        while (System.currentTimeMillis() - start < 1000) {
            Msg m = new Msg("test", "msg", "value", 30000);
            sender.sendMessage(m);
            count++;
        }
        log.info("after 1s - Sent: " + count + " rec: " + rec.get() + " rec2: " + rec2.get());

        while (rec.get() < count) {
            Thread.sleep(100); //waiting for the messages to be received
        }
        log.info("after receiving all - Sent: " + count + " rec: " + rec.get() + " rec2: " + rec2.get());
        //Exclusive test
        rec.set(0);
        rec2.set(0);
        count = 0;
        start = System.currentTimeMillis();
        log.info("reset: " + count + " rec: " + rec.get() + " rec2: " + rec2.get());
        while (System.currentTimeMillis() - start < 1000) {
            Msg m = new Msg("test", "msg", "value", 30000);
            m.setExclusive(true);
            sender.sendMessage(m);
            count++;
        }
        Thread.sleep(100);
        log.info("after 1s Sent exclusive: " + count + " rec: " + rec.get() + " rec2: " + rec2.get());

        while (rec.get() + rec2.get() < count) {
            Thread.sleep(100); //waiting for the messages to be received
        }
        log.info("final numbers: " + count + " rec: " + rec.get() + " rec2: " + rec2.get());
        assert (rec.get() + rec2.get() == count);
    }
}
