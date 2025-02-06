package de.caluga.test.mongo.suite.inmem;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Date;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.annotations.CreationTime;
import de.caluga.morphium.annotations.Index;
import de.caluga.test.mongo.suite.data.UncachedObject;

public class InMemExpireTest extends MorphiumInMemTestBase {

    @Test
    public void noExpireTest() throws Exception {
        morphium.ensureIndicesFor(UCobj.class);;
        UCobj o = new UCobj();
        o.setStrValue("StringValue");
        o.setCreated(null);
        morphium.store(o);
        assertEquals(1, morphium.createQueryFor(UCobj.class).countAll());
        log.info("Waiting for element NOT to be erased...");
        long start = System.currentTimeMillis();

        while (System.currentTimeMillis() - start < 60000) {
            var cnt = morphium.createQueryFor(UCobj.class).countAll();

            if (cnt == 0) {
                System.err.println("Deleted...");
                break;
            }
        }

        long dur = System.currentTimeMillis() - start;
        assertEquals(1, morphium.createQueryFor(UCobj.class).countAll());
        log.info(String.format("still not Deleted after %d ms", dur));
    }

    @Test
    public void expireTest() throws Exception {
        morphium.ensureIndicesFor(UCobj.class);;
        UCobj o = new UCobj();
        o.setStrValue("StringValue");
        o.setCreated(new Date());
        morphium.store(o);
        assertEquals(1, morphium.createQueryFor(UCobj.class).countAll());
        log.info("Waiting for element to be erased...");
        long start = System.currentTimeMillis();

        while (System.currentTimeMillis() - start < 120000) {
            var cnt = morphium.createQueryFor(UCobj.class).countAll();

            if (cnt == 0) {
                System.out.println("Deleted...");
                break;
            }
        }

        long dur = System.currentTimeMillis() - start;
        assertEquals(0, morphium.createQueryFor(UCobj.class).countAll());
        log.info(String.format("Deleted after %d ms", dur));
    }

    @Index(value = { "created" }, options = { "expireAfterSeconds:5" })
    public static class UCobj extends UncachedObject {

        private Date created;

        public Date getCreated() {
            return created;
        }

        public void setCreated(Date created) {
            this.created = created;
        }
    }
}
