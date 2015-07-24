package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.CreationTime;
import de.caluga.morphium.annotations.Index;
import org.junit.Test;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 06.11.13
 * Time: 14:33
 */
@SuppressWarnings("AssertWithSideEffects")
public class ExpireIndexTest extends MongoTest {
    @Test
    public void testExpiry() throws InterruptedException {
        MorphiumSingleton.get().dropCollection(UCobj.class);
        for (int i = 0; i < 100; i++) {
            UCobj u = new UCobj();
            u.setCounter(i);
            u.setValue("V" + i);
            MorphiumSingleton.get().store(u);
        }
        assert (MorphiumSingleton.get().createQueryFor(UCobj.class).countAll() == 100);
        log.info("Waiting for mongo to clear it");
        Thread.sleep(65000);
        assert (MorphiumSingleton.get().createQueryFor(UCobj.class).countAll() == 0);
    }

    @Index(value = {"created"}, options = {"expireAfterSeconds:5"})
    @CreationTime
    public static class UCobj extends UncachedObject {

        @CreationTime
        private Date created;

        public Date getCreated() {
            return created;
        }

        public void setCreated(Date created) {
            this.created = created;
        }
    }
}
