package de.caluga.morphium.server;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TestServer {
    private Logger log = LoggerFactory.getLogger(TestServer.class);
    @Test
    public void startServerTest() throws Exception {
        log.info("Test-Server starting up");
        Runnable r = () -> {

            MorphiumServer srv = new MorphiumServer();
            try {
                srv.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
        r.run();

//        new Thread(r).start();
//        Thread.sleep(1000);
//        SingleMongoConnection con = new SingleMongoConnection();
//        var drv = new DriverMock();
//        drv.setMaxWaitTime(100000);
//        var res = con.connect(drv, "localhost", 27017);
//        Logger logger = LoggerFactory.getLogger(TestServer.class);
//        if (con.isConnected()) {
//            logger.info("Connection established!");
//        } else {
//            logger.error("connection failed");
//        }


    }
}
