package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.CreationTime;
import de.caluga.morphium.annotations.Index;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
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
@Tag("core")
public class ExpireIndexTest extends MultiDriverTestBase {
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testExpiry(Morphium morphium)  throws Exception {
        log.info("==========> Running test with: " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(UCobj.class);
            morphium.ensureIndicesFor(UCobj.class);

            for (int i = 0; i < 100; i++) {
                UCobj u = new UCobj();
                u.setCounter(i);
                u.setStrValue("V" + i);
                morphium.store(u);
            }

            Thread.sleep(500);
            TestUtils.waitForConditionToBecomeTrue(1000, morphium.getDriver().getName() + ": Writing failed?!?!", () -> morphium.createQueryFor(UCobj.class).countAll() == 100);
            log.info("Waiting for mongo to clear it");
            TestUtils.waitForConditionToBecomeTrue(62000, morphium.getDriver().getName() + ": Did not clear?!?!", () -> morphium.createQueryFor(UCobj.class).countAll() == 0);
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
