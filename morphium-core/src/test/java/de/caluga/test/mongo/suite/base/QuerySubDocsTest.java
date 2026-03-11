package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static de.caluga.test.mongo.suite.base.TestUtils.waitForConditionToBecomeTrue;

@Tag("core")
public class QuerySubDocsTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testSubDocs(Morphium morphium) throws Exception {
        SubDocTest sd = new SubDocTest();
        sd.setSubDocs(new HashMap<>());
        morphium.store(sd);
        Thread.sleep(100);
        Query<SubDocTest> q = morphium.createQueryFor(SubDocTest.class).f(SubDocTest.Fields.id).eq(sd.getId());
        q.push("sub_docs.test.subtest", "this value added");
        q.push("sub_docs.test.subtest", "this value added2");
        // Wait for push operations to be visible on replica sets
        waitForConditionToBecomeTrue(10000, "subDocs not updated after push",
            () -> {
                SubDocTest result = q.get();
                return result != null && result.subDocs != null && result.subDocs.size() != 0;
            });
        assert (q.get().subDocs.size() != 0);
    }

    @Entity
    public static class SubDocTest {
        @Id
        private MorphiumId id;
        private Map<String, Map<String, List<String>>> subDocs;

        public Map<String, Map<String, List<String>>> getSubDocs() {
            return subDocs;
        }

        public void setSubDocs(Map<String, Map<String, List<String>>> subDocs) {
            this.subDocs = subDocs;
        }

        public MorphiumId getId() {
            return id;
        }

        public void setId(MorphiumId id) {
            this.id = id;
        }

        public enum Fields {subDocs, id}
    }
}
