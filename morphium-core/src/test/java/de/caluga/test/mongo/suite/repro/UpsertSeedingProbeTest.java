package de.caluga.test.mongo.suite.repro;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.FindAndModifyMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import de.caluga.test.mongo.suite.repro.ReproSupport.Outcome;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Probe for MOR-003 (work package WP-P of docs/research/plan-mor-001-002-003.md): which upsert filter shapes make the
 * server SEED the {@code _id} of the inserted document from the filter?
 * <p>
 * Each shape is sent as a raw {@code findAndModify} with {@code upsert=true} on an empty collection, twice:
 * <ul>
 * <li>variant {@code PINNED_DIFFERENT}: the update carries {@code $setOnInsert._id = "gen"}. If the server seeds the
 * {@code _id} from the filter to a different value, it answers code 66 (immutable field {@code _id}); if it does not
 * seed, the document is inserted under "gen".</li>
 * <li>variant {@code NOT_PINNED}: no {@code _id} in the update; the stored {@code _id} shows what the server seeded
 * (the filter value) or generated (an ObjectId).</li>
 * </ul>
 * The probe asserts nothing about the shapes (it documents the server and the InMemoryDriver); every outcome is logged
 * as {@code REPRO-RESULT}. The table in docs/research/upsert-id-seeding-probe.md is derived from these lines.
 */
@Tag("core")
public class UpsertSeedingProbeTest extends MultiDriverTestBase {

    @Entity(translateCamelCase = false)
    public static class ProbeDoc {
        @Id
        public Object id;
        public String owner;
    }

    private static Map<String, Object> m(Object... kv) {
        Map<String, Object> r = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            r.put((String) kv[i], kv[i + 1]);
        }
        return r;
    }

    private static List<Object> l(Object... v) {
        List<Object> r = new ArrayList<>();
        for (Object o : v) {
            r.add(o);
        }
        return r;
    }

    /** name -> filter. Values X and Y are strings; "gen" is the id pinned by the caller. */
    private static Map<String, Map<String, Object>> shapes() {
        Map<String, Map<String, Object>> s = new LinkedHashMap<>();
        s.put("S01 {_id:X}", m("_id", "X"));
        s.put("S02 {_id:{$eq:X}}", m("_id", m("$eq", "X")));
        s.put("S03 {_id:{$in:[X]}}", m("_id", m("$in", l("X"))));
        s.put("S04 {$and:[{_id:X}]}", m("$and", l(m("_id", "X"))));
        s.put("S05 {$and:[{_id:X},{owner:A}]}", m("$and", l(m("_id", "X"), m("owner", "A"))));
        s.put("S06 {$and:[{_id:{$eq:X}}]}", m("$and", l(m("_id", m("$eq", "X")))));
        s.put("S07 {$and:[{_id:{$in:[X]}}]}", m("$and", l(m("_id", m("$in", l("X"))))));
        s.put("S08 {$and:[{_id:{$in:[X,Y]}}]}", m("$and", l(m("_id", m("$in", l("X", "Y"))))));
        s.put("S09 {$and:[{$and:[{_id:X}]}]}", m("$and", l(m("$and", l(m("_id", "X"))))));
        s.put("S10 {$and:[{_id:X},{$or:[{a:1},{b:2}]}]}", m("$and", l(m("_id", "X"), m("$or", l(m("a", 1), m("b", 2))))));
        s.put("S11 {$or:[{_id:X}]}", m("$or", l(m("_id", "X"))));
        s.put("S12 {$and:[{$or:[{_id:X}]}]}", m("$and", l(m("$or", l(m("_id", "X"))))));
        s.put("S13 {$or:[{_id:X},{_id:Y}]}", m("$or", l(m("_id", "X"), m("_id", "Y"))));
        s.put("S14 {\"_id.a\":1}", m("_id.a", 1));
        s.put("S15 {_id:{a:1}} (literal document)", m("_id", m("a", 1)));
        s.put("S16 {_id:{$gt:\"A\"}}", m("_id", m("$gt", "A")));
        s.put("S17 {$and:[{_id:{$gt:\"A\"}}]}", m("$and", l(m("_id", m("$gt", "A")))));
        s.put("S18 {_id:/re/}", m("_id", Pattern.compile("^X")));
        s.put("S19 {$and:[{_id:X},{_id:Y}]}", m("$and", l(m("_id", "X"), m("_id", "Y"))));
        s.put("S20 {$and:[{_id:X},{_id:X}]}", m("$and", l(m("_id", "X"), m("_id", "X"))));
        s.put("S21 {$nor:[{_id:X}]}", m("$nor", l(m("_id", "X"))));
        s.put("S22 {$and:[{_id:null}]}", m("$and", l(m("_id", null))));
        return s;
    }

    private Map<String, Object> run(Morphium morphium, Map<String, Object> filter, Map<String, Object> update,
                                    Map<String, Object> collation) throws Exception {
        MongoConnection con = morphium.getDriver().getPrimaryConnection(null);
        FindAndModifyMongoCommand cmd = null;
        try {
            cmd = new FindAndModifyMongoCommand(con).setDb(morphium.getConfig().connectionSettings().getDatabase())
                    .setColl(morphium.getMapper().getCollectionName(ProbeDoc.class)).setQuery(Doc.of(filter))
                    .setUpdate(Doc.of(update)).setUpsert(true).setNewFlag(true);
            if (collation != null) {
                cmd.setCollation(collation);
            }
            return cmd.execute();
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            } else {
                morphium.getDriver().releaseConnection(con);
            }
        }
    }

    private String stored(Morphium morphium) {
        List<Map<String, Object>> docs = morphium.createQueryFor(ProbeDoc.class).asMapList();
        List<String> ids = new ArrayList<>();
        for (Map<String, Object> d : docs) {
            Object id = d.get("_id");
            ids.add(id == null ? "null" : id.getClass().getSimpleName() + ":" + id);
        }
        return docs.size() + " doc(s), _id=" + ids;
    }

    private void probe(Morphium morphium, String name, Map<String, Object> filter, Map<String, Object> collation) {
        String driver = morphium.getDriver().getName();
        String suffix = collation == null ? "" : " [collation]";

        morphium.dropCollection(ProbeDoc.class);
        Map<String, Object> pinned = m("$set", m("owner", "A"), "$setOnInsert", m("_id", "gen"));
        Outcome<Map<String, Object>> a = ReproSupport.attempt(() -> run(morphium, filter, pinned, collation));
        ReproSupport.report(driver, "PROBE PINNED_DIFFERENT " + name + suffix,
                            (a.failed() ? "FAILED " : "OK ") + a.describe() + " | stored: " + stored(morphium));

        morphium.dropCollection(ProbeDoc.class);
        Map<String, Object> plain = m("$set", m("owner", "A"));
        Outcome<Map<String, Object>> b = ReproSupport.attempt(() -> run(morphium, filter, plain, collation));
        ReproSupport.report(driver, "PROBE NOT_PINNED " + name + suffix,
                            (b.failed() ? "FAILED " : "OK ") + b.describe() + " | stored: " + stored(morphium));
        morphium.dropCollection(ProbeDoc.class);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void upsertIdSeedingMatrix(Morphium morphium) throws Exception {
        try (morphium) {
            assertFalse(shapes().isEmpty());
            for (Map.Entry<String, Map<String, Object>> e : shapes().entrySet()) {
                probe(morphium, e.getKey(), e.getValue(), null);
            }
            // collation: strength 2 makes "x" equal to "X"; does the server still seed _id from the equality?
            Map<String, Object> collation = m("locale", "en", "strength", 2);
            probe(morphium, "C01 {_id:\"x\"}", m("_id", "x"), collation);
            probe(morphium, "C02 {$and:[{_id:\"x\"}]}", m("$and", l(m("_id", "x"))), collation);
            assertEquals(0, morphium.createQueryFor(ProbeDoc.class).countAll());
        }
    }
}
