package de.caluga.test.mongo.suite.base;


import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.morphium.server.MorphiumServer;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicLong;

public class MorphiumServerTest {
    private Logger log = LoggerFactory.getLogger(MorphiumServerTest.class);


    @Test
    public void singleConnectToServerTest()throws Exception {
        var srv = new MorphiumServer(17017, "localhost", 100, 1);
        srv.start();
        SingleMongoConnectDriver drv = new SingleMongoConnectDriver();
        drv.setHostSeed("localhost:17017");
        drv.setMaxConnections(10);
        drv.setHeartbeatFrequency(250);
        drv.connect();
        log.info("connection established");
        SingleMongoConnectDriver drv2 = new SingleMongoConnectDriver();
        drv2.setHostSeed("localhost:17017");
        drv2.setMaxConnections(10);
        drv2.setHeartbeatFrequency(250);
        drv2.connect();
        log.info("connection established");
        drv.close();
        drv2.close();
        srv.terminate();
    }

    @Test
    public void testServerMessaging() throws Exception {
        var srv = new MorphiumServer(17017, "localhost", 100, 1);
        srv.start();
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:17017");
        cfg.setDatabase("srvtst");
        cfg.setMaxConnections(10);
        Morphium morphium = new Morphium(cfg);
        final AtomicLong recTime = new AtomicLong(0l);

        try(morphium) {
            //Messaging test
            var msg1 = new Messaging(morphium);
            msg1.start();
            var msg2 = new Messaging(morphium);
            msg2.start();
            msg2.addListenerForMessageNamed("tstmsg", new MessageListener() {
                @Override
                public Msg onMessage(Messaging msg, Msg m) {
                    recTime.set(System.currentTimeMillis());
                    log.info("incoming mssage");
                    return null;
                }
            });

            long start = System.currentTimeMillis();
            msg1.sendMessage(new Msg("tstmsg", "hello", "value"));

            while (recTime.get() == 0L) {
                Thread.yield();
            }

            long dur = recTime.get() - start;
            log.info("Got message after {}ms", dur);
        }

        srv.terminate();
    }

    @Test
    public void testServerMessagingMoreThanOne() throws Exception {
        var srv = new MorphiumServer(17017, "localhost", 100, 1);
        srv.start();
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:17017");
        cfg.setDatabase("srvtst");
        cfg.setMaxConnections(10);
        Morphium morphium = new Morphium(cfg);
        MorphiumConfig cfg2 = new MorphiumConfig();
        cfg2.setHostSeed("localhost:17017");
        cfg2.setDatabase("srvtst");
        cfg2.setMaxConnections(10);
        Morphium morphium2 = new Morphium(cfg2);
        final AtomicLong recTime = new AtomicLong(0l);

        try(morphium) {
            //Messaging test
            var msg1 = new Messaging(morphium);
            msg1.start();
            var msg2 = new Messaging(morphium2);
            msg2.start();
            msg2.addListenerForMessageNamed("tstmsg", new MessageListener() {
                @Override
                public Msg onMessage(Messaging msg, Msg m) {
                    recTime.set(System.currentTimeMillis());
                    log.info("incoming mssage");
                    return null;
                }
            });

            long start = System.currentTimeMillis();
            msg1.sendMessage(new Msg("tstmsg", "hello", "value"));

            while (recTime.get() == 0L) {
                Thread.yield();
            }

            long dur = recTime.get() - start;
            log.info("Got message after {}ms", dur);
        }

        srv.terminate();
    }

    @Test
    public void testServer() throws Exception {
        var srv = new MorphiumServer(17017, "localhost", 100, 1);
        srv.start();
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setHostSeed("localhost:17017");
        cfg.setDatabase("srvtst");
        cfg.setMaxConnections(10);
        Morphium morphium = new Morphium(cfg);
        final AtomicLong recTime = new AtomicLong(0l);

        try(morphium) {
            for (int i = 0; i < 100; i++) {
                UncachedObject uc = new UncachedObject();
                uc.setCounter(i);
                uc.setStrValue("Counter-" + i);
                morphium.store(uc);
            }

            var lst = morphium.createQueryFor(UncachedObject.class).asList();
            assertEquals(100, lst.size());
        }

        srv.terminate();
    }
}
