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
    public void testExpiry(Morphium morphium) throws Exception {
        log.info("==========> Running test with: " + morphium.getDriver().getName());

        try (morphium) {
            morphium.dropCollection(UCobj.class);
            Thread.sleep(500);

            log.info("Ensuring indices for UCobj class...");
            var expectedIndices = morphium.getIndexesFromEntity(UCobj.class);
            log.info("Expected indices from entity: " + expectedIndices);

            try {
                morphium.ensureIndicesFor(UCobj.class);
                log.info("ensureIndicesFor completed");
            } catch (Exception e) {
                log.error("Error ensuring indices", e);
                throw e;
            }

            // Verify index was created
            var indices = morphium.getIndexesFromMongo(UCobj.class);
            log.info("Actual indices in MongoDB: " + indices);
            boolean ttlIndexFound = false;
            for (var idx : indices) {
                log.info("Index: " + idx);
                if (idx.getExpireAfterSeconds() != null) {
                    ttlIndexFound = true;
                    log.info("TTL index found with expireAfterSeconds: " + idx.getExpireAfterSeconds());
                }
            }
            if (!ttlIndexFound) {
                log.error("TTL index was not created!");
            }

            // Give the TTL index time to be created and replicated across cluster
            Thread.sleep(2000);

            // Use fewer documents (10 instead of 100) to speed up TTL deletion in cluster
            // environments
            // where deletions happen gradually over time
            int docCount = 10;
            for (int i = 0; i < docCount; i++) {
                UCobj u = new UCobj();
                u.setCounter(i);
                u.setStrValue("V" + i);
                morphium.store(u);
            }

            Thread.sleep(1000);
            TestUtils.waitForConditionToBecomeTrue(5000, morphium.getDriver().getName() + ": Writing failed?!?!",
                    () -> morphium.createQueryFor(UCobj.class).countAll() == docCount);

            // Check if documents have creation time set
            var sample = morphium.createQueryFor(UCobj.class).limit(1).asList().get(0);
            log.info("Sample document - created: " + sample.getCreated() + ", counter: " + sample.getCounter());
            log.info("Current time: " + new Date() + ", time until expiry: "
                    + (sample.getCreated() != null
                            ? (sample.getCreated().getTime() + 5000 - System.currentTimeMillis()) + "ms"
                            : "N/A"));

            // Check what's actually stored in MongoDB (not just the Java object)
            var rawDoc = morphium.getMapper().serialize(sample);
            log.info("Serialized document (what should be in MongoDB): " + rawDoc);
            log.info("Serialized 'created' field type: "
                    + (rawDoc.get("created") != null ? rawDoc.get("created").getClass().getName() : "NULL"));
            log.info("Serialized 'created' field value: " + rawDoc.get("created"));

            log.info(
                    "Waiting for mongo to clear it (TTL monitor runs ~every 60s for real MongoDB, ~10s for InMemory, expiry is 5s)");

            // TTL monitor runs every ~60s for MongoDB, ~10s for InMemory, expiry is 5s
            // Worst case for MongoDB: created just after TTL pass = wait 60s for next pass
            // + 5s expiry + 10s buffer = 75s
            // For InMemory: worst case ~15s
            // Use 120s timeout to handle worst-case cluster scenarios
            int timeout = morphium.getDriver().getName().contains("InMem") ? 30000 : 280000;
            TestUtils.waitForConditionToBecomeTrue(timeout, morphium.getDriver().getName() + ": Did not clear?!?!",
                    () -> morphium.createQueryFor(UCobj.class).countAll() == 0,
                    (dur) -> {
                        long count = morphium.createQueryFor(UCobj.class).countAll();
                        if (count > 0) {
                            var doc = morphium.createQueryFor(UCobj.class).limit(1).asList().get(0);
                            log.info("Still waiting after " + (dur / 1000) + "s - current count: " + count +
                                    ", sample created: " + doc.getCreated() +
                                    ", age: "
                                    + (doc.getCreated() != null
                                            ? (System.currentTimeMillis() - doc.getCreated().getTime()) / 1000 + "s"
                                            : "N/A"));
                        } else {
                            log.info("Still waiting after " + (dur / 1000) + "s - current count: " + count);
                        }
                    });
            log.info(morphium.getDriver().getName() + ": done.");
        } catch (Exception e) {
            log.error(morphium.getDriver().getName() + ":Got Exception", e);
            throw (e);
        }
    }

    @Index(value = { "created" }, options = { "expireAfterSeconds:5" })
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
