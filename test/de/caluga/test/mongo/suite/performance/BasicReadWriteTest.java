package de.caluga.test.mongo.suite.performance;/**
 * Created by stephan on 21.10.15.
 */

import de.caluga.test.mongo.suite.MongoTest;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO: Add Documentation here
 **/
public class BasicReadWriteTest extends MongoTest {
    @Test
    public void basicReadTest() {
        morphium.dropCollection(UncachedObject.class);

        createUncachedObjects(10000);

        log.info("Reading data...");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq((int) (Math.random() * 10000.0));
        }
        long dur = System.currentTimeMillis() - start;
        log.info("Reading random took " + dur + "ms");

        log.info("Now reading sequentially...");
        start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(i);
        }
        dur = System.currentTimeMillis() - start;
        log.info("Reading sequential took " + dur + "ms");
    }

    @Test
    public void basicWriteTest() {
        log.info("\n\n\nDoing writes with index check off");
        morphium.getConfig().setAutoIndexAndCappedCreationOnWrite(false);
        doWriteTest();

        log.info("\n\n\nDoing writes with index check ON");
        morphium.getConfig().setAutoIndexAndCappedCreationOnWrite(true);
        doWriteTest();
    }

    private void doWriteTest() {
        morphium.dropCollection(UncachedObject.class);
        log.info("Creating objects sequentially...");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setValue("V" + i);
            morphium.store(uc);
            if (i % 1000 == 0) log.info("got " + i);
        }
        long dur = System.currentTimeMillis() - start;
        log.info("Took: " + dur + "ms");

        log.info("Creating objects randomly..");
        start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            UncachedObject uc = new UncachedObject();
            int c = (int) (Math.random() * 100000.0);
            uc.setCounter(c);
            uc.setValue("V" + c);
            morphium.store(uc);
            if (i % 1000 == 0) log.info("got " + i);

        }
        dur = System.currentTimeMillis() - start;
        log.info("Took: " + dur + "ms");


        log.info("Block writing...");
        List<UncachedObject> buffer = new ArrayList<>();
        start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            UncachedObject uc = new UncachedObject();
            int c = (int) (Math.random() * 100000.0);
            uc.setCounter(c);
            uc.setValue("n" + c);
            buffer.add(uc);
        }
        morphium.storeList(buffer);
        dur = System.currentTimeMillis() - start;
        log.info("Took: " + dur + "ms");
    }
}
