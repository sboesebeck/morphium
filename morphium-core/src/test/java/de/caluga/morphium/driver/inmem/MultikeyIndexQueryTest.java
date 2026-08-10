package de.caluga.morphium.driver.inmem;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import de.caluga.morphium.driver.Doc;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An index on an array field must never change what a query returns (#289).
 *
 * <p>{@code CollectionIndexStore} does not implement multikey indexing - a terminal {@code List}
 * becomes ONE key holding the whole list (see {@link IndexKey#extract}). An equality query builds
 * a scalar lookup key, which can never match that, so once {@code find()}/{@code count()} started
 * being served from an index the query silently returned nothing. Real-world shape: {@code Msg}
 * carries {@code @Index} on {@code processedBy} plus compound indexes over {@code processed_by},
 * and a downstream planner selecting work with {@code processed_by == "Planner"} found none.
 *
 * <p>Until multikey indexing exists, such an index must not be used for lookups at all - the scan
 * that {@code QueryHelper} performs evaluates MongoDB's array-membership semantics correctly.
 */
@Tag("core")
public class MultikeyIndexQueryTest {

    private static final String DB = "multikey_db";

    private List<Map<String, Object>> docs() {
        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(Doc.of("_id", 1, "processed_by", new ArrayList<>(List.of("Planner")), "priority", 545));
        docs.add(Doc.of("_id", 2, "processed_by", new ArrayList<String>(), "priority", 100));
        docs.add(Doc.of("_id", 3, "processed_by", new ArrayList<>(List.of("other", "Planner")), "priority", 100));
        return docs;
    }

    private List<Object> ids(List<Map<String, Object>> res) {
        List<Object> ids = new ArrayList<>();
        for (var d : res) {
            ids.add(d.get("_id"));
        }
        return ids;
    }

    @Test
    public void indexedArrayFieldAnswersEqualityLikeAnUnindexedOne() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        try {
            drv.createIndex(DB, "indexed", Doc.of("processed_by", 1), Doc.of("name", "pb_1"));
            drv.createIndex(DB, "indexed", Doc.of("processed_by", 1, "priority", 1), Doc.of("name", "pb_prio_1"));
            drv.store(DB, "indexed", docs(), null);
            drv.store(DB, "plain", docs(), null);

            // array membership: mongod matches a document whose array CONTAINS the value
            assertThat(ids(drv.find(DB, "plain", Doc.of("processed_by", "Planner"), null, null, 0, 0)))
                .as("baseline without an index")
                .containsExactlyInAnyOrder(1, 3);
            assertThat(ids(drv.find(DB, "indexed", Doc.of("processed_by", "Planner"), null, null, 0, 0)))
                .as("an index on the array field must not change the result (#289)")
                .containsExactlyInAnyOrder(1, 3);

            // compound index over the array field plus a scalar
            assertThat(ids(drv.find(DB, "indexed", Doc.of("processed_by", "Planner", "priority", 545), null, null, 0, 0)))
                .as("compound index whose leading field is an array")
                .containsExactly(1);

            // these two always worked and must keep working - the messaging poll uses exactly them
            assertThat(ids(drv.find(DB, "indexed", Doc.of("processed_by.0", Doc.of("$exists", false)), null, null, 0, 0)))
                .as("empty-array probe")
                .containsExactly(2);
            assertThat(ids(drv.find(DB, "indexed", Doc.of("processed_by", Doc.of("$ne", "x")), null, null, 0, 0)))
                .as("$ne on an array field")
                .containsExactlyInAnyOrder(1, 2, 3);

            // counts must agree with finds
            assertThat(drv.count(DB, "indexed", Doc.of("processed_by", "Planner"), null, null))
                .as("count must agree with find")
                .isEqualTo(2);

            // a scalar field on the same collection still gets index-backed lookups
            assertThat(ids(drv.find(DB, "indexed", Doc.of("priority", 545), null, null, 0, 0)))
                .containsExactly(1);
        } finally {
            drv.close();
        }
    }
}
