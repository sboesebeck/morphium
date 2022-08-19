package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.CreationTime;
import de.caluga.morphium.annotations.Index;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 06.11.13
 * Time: 14:33
 */
@SuppressWarnings("AssertWithSideEffects")
public class ExpireIndexTest extends MultiDriverTestBase {
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testExpiry(Morphium morphium) throws InterruptedException {
        try (morphium) {
            morphium.dropCollection(UCobj.class);
            for (int i = 0; i < 100; i++) {
                UCobj u = new UCobj();
                u.setCounter(i);
                u.setStrValue("V" + i);
                morphium.store(u);
            }
            Thread.sleep(500);
            waitForCondidtionToBecomeTrue(1000, "Writing failed?!?!", () -> morphium.createQueryFor(UCobj.class).countAll() == 100);
            log.info("Waiting for mongo to clear it");
            waitForCondidtionToBecomeTrue(62000, "Did not clear?!?!", () -> morphium.createQueryFor(UCobj.class).countAll() == 0);
            log.info("done.");
        }
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
