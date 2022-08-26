package de.caluga.morphium.server;

import de.caluga.morphium.driver.wire.SingleMongoConnection;
import de.caluga.test.DriverMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TestServer {
    @Test
    public void startServerTest() throws Exception {
        Runnable r = () -> {

            MorphiumServer srv = new MorphiumServer();
            try {
                srv.start(17017);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
//        r.run();

//        new Thread(r).start();
//        Thread.sleep(1000);
        SingleMongoConnection con = new SingleMongoConnection();
        var drv = new DriverMock();
        drv.setMaxWaitTime(100000);
        var res = con.connect(drv, "localhost", 27017);
        Logger logger = LoggerFactory.getLogger(TestServer.class);
        if (con.isConnected()) {
            logger.info("Connection established!");
        } else {
            logger.error("connection failed");
        }


    }
}
