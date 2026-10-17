package de.caluga.test.mongo.suite.contract;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.bulk.MorphiumBulkContext;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * MOR-005: audit of the Morphium write paths (Query.set/inc/dec/push/pull/..., Morphium entity and bulk variants). For each path two facts are measured on every
 * driver: does it raise when the server rejects the statement (the update command reports errors in its reply instead of throwing), and does it translate the
 * Java field name to the stored name. The table is in docs/research/audit-write-paths.md.
 */
@Tag("core")
public class WritePathAuditContractTest extends MultiDriverTestBase {

    public enum F {
        counterValue, tagList, code, lastSeen
    }

    @de.caluga.morphium.annotations.Embedded
    public static class Box {
        public int innerValue;
        public Date innerDate;
    }

    @Entity
    public static class Audit {
        @Id
        public String id;
        public String code;
        public int scalarValue;
        public int counterValue;
        public List<String> tagList;
        public Date lastSeen;
        public Box box;
    }

    @FunctionalInterface
    interface Op {
        void run(Morphium m, Query<Audit> q, Audit a) throws Exception;
    }

    /** a write path with the effect it must have on the stored document (stored names) */
    record Probe(String name, Op op, Predicate<Map<String, Object>> effect) {
    }

    private static final String[] JAVA_NAMES = {"counterValue", "tagList", "lastSeen", "scalarValue"};

    private static final Predicate<Map<String, Object>> NOTHING = raw -> true;

    private static Predicate<Map<String, Object>> number(String key, double expected) {
        return raw -> raw.get(key) instanceof Number n && n.doubleValue() == expected;
    }

    private static Predicate<Map<String, Object>> box(String key, double expected) {
        return raw -> raw.get("box") instanceof Map<?, ?> b && b.get(key) instanceof Number n && n.doubleValue() == expected;
    }

    private static Predicate<Map<String, Object>> list(String key, List<String> expected) {
        return raw -> expected.equals(raw.get(key));
    }

    /** the paths of the audit; the error expectations are measured with unique indexes on code, counter_value and tag_list */
    private static final List<Probe> PROBES = List.of(
        new Probe("Query.set(field)", (m, q, a) -> q.set("counterValue", 5), number("counter_value", 5)),
        new Probe("Query.set(map)", (m, q, a) -> q.set(Map.of("counterValue", 5)), number("counter_value", 5)),
        new Probe("Query.setEnum(map)", (m, q, a) -> q.setEnum(Map.of(F.counterValue, 5)), number("counter_value", 5)),
        new Probe("Query.set(enum)", (m, q, a) -> q.set(F.counterValue, 5), number("counter_value", 5)),
        new Probe("Query.set(dotted path)", (m, q, a) -> q.set("box.innerValue", 5), box("inner_value", 5)),
        new Probe("Query.unset(field)", (m, q, a) -> q.unset("code"), raw -> !raw.containsKey("code")),
        new Probe("Query.unset(enum)", (m, q, a) -> q.unset(F.code), raw -> !raw.containsKey("code")),
        new Probe("Query.inc(field,Integer)", (m, q, a) -> q.inc("counterValue", Integer.valueOf(4)), number("counter_value", 5)),
        new Probe("Query.inc(field,Long)", (m, q, a) -> q.inc("counterValue", Long.valueOf(4)), number("counter_value", 5)),
        new Probe("Query.inc(field,Double)", (m, q, a) -> q.inc("counterValue", Double.valueOf(4)), number("counter_value", 5)),
        new Probe("Query.inc(field,Number)", (m, q, a) -> q.inc("counterValue", (Number) Integer.valueOf(4)), number("counter_value", 5)),
        new Probe("Query.inc(enum)", (m, q, a) -> q.inc(F.counterValue, Integer.valueOf(4)), number("counter_value", 5)),
        new Probe("Query.inc(dotted path)", (m, q, a) -> q.inc("box.innerValue", Integer.valueOf(4)), box("inner_value", 5)),
        new Probe("Query.dec(field,Integer)", (m, q, a) -> q.dec("counterValue", Integer.valueOf(-4)), number("counter_value", 5)),
        new Probe("Query.dec(field,Number)", (m, q, a) -> q.dec("counterValue", (Number) Integer.valueOf(-4)), number("counter_value", 5)),
        new Probe("Morphium.inc(query,map)", (m, q, a) -> m.inc(q, Map.of("counterValue", 4), false, false, null), number("counter_value", 5)),
        new Probe("Morphium.inc(enumMap,query)", (m, q, a) -> m.inc(Map.of(F.counterValue, 4), q, false, false, null), number("counter_value", 5)),
        new Probe("Morphium.dec(query,map)", (m, q, a) -> m.dec(q, Map.of("counterValue", -4), false, false, null), number("counter_value", 5)),
        new Probe("Query.push(field)", (m, q, a) -> q.push("tagList", "x"), list("tag_list", List.of("a", "x"))),
        new Probe("Query.push(enum)", (m, q, a) -> q.push(F.tagList, "x"), list("tag_list", List.of("a", "x"))),
        new Probe("Query.pushAll(field)", (m, q, a) -> q.pushAll("tagList", List.of("x")), list("tag_list", List.of("a", "x"))),
        new Probe("Query.pull(field)", (m, q, a) -> q.pull("tagList", "a"), list("tag_list", List.of())),
        new Probe("Query.pullAll(field)", (m, q, a) -> q.pullAll("tagList", List.of("a")), list("tag_list", List.of())),
        new Probe("Morphium.addToSet", (m, q, a) -> m.addToSet(q, "tagList", "x"), list("tag_list", List.of("a", "x"))),
        new Probe("Morphium.currentDate(field)", (m, q, a) -> m.currentDate(q, "lastSeen", false, false), raw -> raw.get("last_seen") instanceof Date),
        new Probe("Morphium.currentDate(enum)", (m, q, a) -> m.currentDate(q, F.lastSeen, false, false), raw -> raw.get("last_seen") instanceof Date),
        new Probe("Morphium.currentDate(dotted path)", (m, q, a) -> m.currentDate(q, "box.innerDate", false, false), raw -> raw.get("box") instanceof Map<?, ?> b && b.get("inner_date") instanceof Date),
        new Probe("Morphium.setInEntity(field)", (m, q, a) -> m.setInEntity(a, "counterValue", 5), number("counter_value", 5)),
        new Probe("Morphium.inc(entity)", (m, q, a) -> m.inc(a, "counterValue", 4), number("counter_value", 5)),
        new Probe("Morphium.dec(entity)", (m, q, a) -> m.dec(a, "counterValue", -4), number("counter_value", 5)),
        new Probe("Morphium.unsetInEntity", (m, q, a) -> m.unsetInEntity(a, "code"), raw -> !raw.containsKey("code")),
        new Probe("Morphium.push(entity)", (m, q, a) -> m.push(a, "tagList", "x", false), list("tag_list", List.of("a", "x"))),
        new Probe("Bulk.addSetRequest", (m, q, a) -> bulk(m, b -> b.addSetRequest(q, "counterValue", 5, false, false)), number("counter_value", 5)),
        new Probe("Bulk.addIncRequest", (m, q, a) -> bulk(m, b -> b.addIncRequest(q, "counterValue", 4, false, false)), number("counter_value", 5)),
        new Probe("Bulk.addPushRequest", (m, q, a) -> bulk(m, b -> b.addPushRequest(q, "tagList", "x", false, false)), list("tag_list", List.of("a", "x"))),
        new Probe("Bulk.addUnsetRequest", (m, q, a) -> bulk(m, b -> b.addUnsetRequest(q, "code", 1, false, false)), raw -> !raw.containsKey("code")),
        new Probe("Bulk.addCurrentDateRequest", (m, q, a) -> bulk(m, b -> b.addCurrentDateRequest(q, false, false, "lastSeen")), raw -> raw.get("last_seen") instanceof Date));

    /**
     * What a probe must do when the server rejects it: the arguments of {@link #FAILING} are chosen so that each of them violates a unique index (document b holds
     * counter 2 and tag b, document a has code x), the dotted paths go below a scalar and pull/pullAll target a field that is no array. Every path raises since
     * MOR-059, except the ones that are still silent: unsetInEntity (step 4 of the plan) and the bulk requests (step 5); pinned deviation MOR-059.
     */
    private static final Set<String> SILENT_UNTIL_MOR_059_IS_DONE = Set.of("Bulk.addSetRequest", "Bulk.addIncRequest", "Bulk.addPushRequest", "Bulk.addUnsetRequest", "Bulk.addCurrentDateRequest");

    /** raise today with an exception of their own ("Update failed"), not with the write errors of the server; replaced in step 4 of the plan */
    private static final Set<String> LEGACY_EXCEPTION = Set.of();

    /** the server error code of each rejected probe; 11000 (duplicate key) unless listed */
    private static final Map<String, Integer> CODE = Map.of("Query.pull(field)", 2, "Query.pullAll(field)", 2, "Query.set(dotted path)", 28, "Query.inc(dotted path)", 28,
            "Morphium.currentDate", 28, "Bulk.addCurrentDateRequest", 28);

    /** the arguments that make a probe fail on the server */
    private static final Map<String, Op> FAILING = new LinkedHashMap<>();

    static {
        FAILING.put("Query.set(field)", (m, q, a) -> q.set("counterValue", 2));
        FAILING.put("Query.set(map)", (m, q, a) -> q.set(Map.of("counterValue", 2)));
        FAILING.put("Query.setEnum(map)", (m, q, a) -> q.setEnum(Map.of(F.counterValue, 2)));
        FAILING.put("Query.set(enum)", (m, q, a) -> q.set(F.counterValue, 2));
        FAILING.put("Query.set(dotted path)", (m, q, a) -> q.set("scalar_value.sub", 1));
        FAILING.put("Query.unset(field)", (m, q, a) -> q.unset("code"));
        FAILING.put("Query.unset(enum)", (m, q, a) -> q.unset(F.code));
        FAILING.put("Morphium.inc(query,map)", (m, q, a) -> m.inc(q, Map.of("counter_value", 1), false, false, null));
        // inc(enumMap, query) cannot be measured on its own: the keys are Java names and are Java names, so a stored name would be needed to provoke a collision; the translation of this path is covered by the translation probes
        // dec by -1 adds 1, which collides with the counter of document b
        FAILING.put("Morphium.dec(query,map)", (m, q, a) -> m.dec(q, Map.of("counter_value", -1), false, false, null));
        FAILING.put("Bulk.addCurrentDateRequest", (m, q, a) -> bulk(m, b -> b.addCurrentDateRequest(q, false, false, "scalar_value.sub")));
        FAILING.put("Query.inc(field,Integer)", (m, q, a) -> q.inc("counterValue", Integer.valueOf(1)));
        FAILING.put("Query.inc(field,Long)", (m, q, a) -> q.inc("counterValue", Long.valueOf(1)));
        FAILING.put("Query.inc(field,Double)", (m, q, a) -> q.inc("counterValue", Double.valueOf(1)));
        FAILING.put("Query.inc(field,Number)", (m, q, a) -> q.inc("counterValue", (Number) Integer.valueOf(1)));
        FAILING.put("Query.inc(enum)", (m, q, a) -> q.inc(F.counterValue, Integer.valueOf(1)));
        FAILING.put("Query.inc(dotted path)", (m, q, a) -> q.inc("scalar_value.sub", Integer.valueOf(1)));
        FAILING.put("Morphium.currentDate", (m, q, a) -> m.currentDate(q, "scalar_value.sub", false, false));
        FAILING.put("Query.dec(field,Integer)", (m, q, a) -> q.dec("counterValue", Integer.valueOf(-1)));
        FAILING.put("Query.dec(field,Number)", (m, q, a) -> q.dec("counterValue", (Number) Integer.valueOf(-1)));
        FAILING.put("Query.push(field)", (m, q, a) -> q.push("tagList", "b"));
        FAILING.put("Query.push(enum)", (m, q, a) -> q.push(F.tagList, "b"));
        FAILING.put("Query.pushAll(field)", (m, q, a) -> q.pushAll("tagList", List.of("b")));
        FAILING.put("Query.pull(field)", (m, q, a) -> q.pull("code", "x"));
        FAILING.put("Query.pullAll(field)", (m, q, a) -> q.pullAll("code", List.of("x")));
        FAILING.put("Morphium.addToSet", (m, q, a) -> m.addToSet(q, "tagList", "b"));
        FAILING.put("Morphium.inc(entity)", (m, q, a) -> m.inc(a, "counterValue", 1));
        FAILING.put("Morphium.dec(entity)", (m, q, a) -> m.dec(a, "counterValue", -1));
        FAILING.put("Morphium.setInEntity(field)", (m, q, a) -> m.setInEntity(a, "counterValue", 2));
        FAILING.put("Morphium.unsetInEntity", (m, q, a) -> m.unsetInEntity(a, "code"));
        FAILING.put("Morphium.push(entity)", (m, q, a) -> m.push(a, "tagList", "b", false));
        FAILING.put("Bulk.addSetRequest", (m, q, a) -> bulk(m, b -> b.addSetRequest(q, "counterValue", 2, false, false)));
        FAILING.put("Bulk.addIncRequest", (m, q, a) -> bulk(m, b -> b.addIncRequest(q, "counterValue", 1, false, false)));
        FAILING.put("Bulk.addPushRequest", (m, q, a) -> bulk(m, b -> b.addPushRequest(q, "tagList", "b", false, false)));
        FAILING.put("Bulk.addUnsetRequest", (m, q, a) -> bulk(m, b -> b.addUnsetRequest(q, "code", 1, false, false)));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void bulk(Morphium m, java.util.function.Consumer<MorphiumBulkContext<Audit>> requests) {
        MorphiumBulkContext<Audit> b = m.createBulkRequestContext(Audit.class, false);
        requests.accept(b);
        b.runBulk();
    }

    private void setup(Morphium m, boolean indexes) throws Exception {
        m.dropCollection(Audit.class);

        if (indexes) {
            String coll = m.getMapper().getCollectionName(Audit.class);
            m.ensureIndex(Audit.class, coll, Map.of("code", 1), Map.of("unique", true));
            m.ensureIndex(Audit.class, coll, Map.of("counter_value", 1), Map.of("unique", true));
            m.ensureIndex(Audit.class, coll, Map.of("tag_list", 1), Map.of("unique", true));
        }

        Audit a = new Audit();
        a.id = "a";
        a.code = "x";
        a.scalarValue = 7;
        a.counterValue = 1;
        a.tagList = new ArrayList<>(List.of("a"));
        a.box = new Box();
        a.box.innerValue = 1;
        m.store(a);
        Audit b = new Audit();
        b.id = "b";
        b.scalarValue = 7;
        b.counterValue = 2;
        b.tagList = new ArrayList<>(List.of("b"));
        m.store(b);
    }

    private Map<String, Object> raw(Morphium m, String id) throws Exception {
        return ModifyFixturesAccess.rawDocs(m.createQueryFor(Audit.class).f("_id").eq(id)).get(0);
    }

    private static boolean hasJavaNames(Map<String, Object> raw) {
        for (String n : JAVA_NAMES) {
            if (raw.containsKey(n)) {
                return true;
            }
        }

        return raw.get("box") instanceof Map<?, ?> b && (b.containsKey("innerValue") || b.containsKey("innerDate"));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aRejectedWriteIsRaised(Morphium m) throws Exception {
        try (m) {
            // the unique indexes and the errors are server behaviour: measured on a real MongoDB only
            assumeFalse(UpdateContract.isInMem(m), "needs a real MongoDB");
            List<String> problems = new ArrayList<>();

            for (Map.Entry<String, Op> e : FAILING.entrySet()) {
                setup(m, true);
                Audit a = m.createQueryFor(Audit.class).f("_id").eq("a").get();
                Query<Audit> q = m.createQueryFor(Audit.class).f("_id").eq("a");
                Map<String, Object> before = raw(m, "a");
                boolean raised = false;
                Throwable thrown = null;

                try {
                    e.getValue().run(m, q, a);
                } catch (Throwable t) {
                    raised = true;
                    thrown = t;
                }

                boolean expected = !SILENT_UNTIL_MOR_059_IS_DONE.contains(e.getKey());

                if (raised != expected) {
                    problems.add(e.getKey() + ": raised=" + raised + ", expected " + expected);
                } else if (raised && !LEGACY_EXCEPTION.contains(e.getKey())) {
                    if (!(thrown instanceof MorphiumDriverException mde)) {
                        problems.add(e.getKey() + ": expected a MorphiumDriverException but got " + thrown);
                    } else {
                        int code = CODE.getOrDefault(e.getKey(), 11000);

                        if (!(mde.getMongoCode() instanceof Number n) || n.intValue() != code) {
                            problems.add(e.getKey() + ": expected the server code " + code + " but got " + mde.getMongoCode() + " (" + mde.getMessage() + ")");
                        }

                        if (code == 11000 && !mde.isDuplicateKey()) {
                            problems.add(e.getKey() + ": isDuplicateKey() is false");
                        }

                        if (mde.getWriteErrors() == null || mde.getWriteErrors().isEmpty()) {
                            problems.add(e.getKey() + ": no write errors on the exception");
                        }
                    }
                }

                // a rejected statement changes nothing
                if (!raw(m, "a").equals(before)) {
                    problems.add(e.getKey() + ": the document changed " + raw(m, "a"));
                }
            }

            assertTrue(problems.isEmpty(), "\n" + String.join("\n", problems));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void javaNamesAreTranslatedToTheStoredNames(Morphium m) throws Exception {
        try (m) {
            List<String> problems = new ArrayList<>();

            for (Probe p : PROBES) {
                setup(m, false);
                Audit a = m.createQueryFor(Audit.class).f("_id").eq("a").get();
                Query<Audit> q = m.createQueryFor(Audit.class).f("_id").eq("a");

                try {
                    p.op().run(m, q, a);
                } catch (Throwable t) {
                    // the dotted paths below a scalar are rejected by a real MongoDB and by the InMemoryDriver update command; the effect is checked on the stray names only
                }

                Map<String, Object> raw = raw(m, "a");
                if (hasJavaNames(raw)) {
                    problems.add(p.name() + ": stray Java name in " + new java.util.TreeMap<>(raw));
                } else if (!p.effect().test(raw)) {
                    problems.add(p.name() + ": the effect is missing in " + new java.util.TreeMap<>(raw));
                }
            }

            assertTrue(problems.isEmpty(), "\n" + String.join("\n", problems));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void decWithAMapDecrements(Morphium m) throws Exception {
        try (m) {
            setup(m, false);
            m.dec(m.createQueryFor(Audit.class).f("_id").eq("a"), Map.of("counter_value", 1), false, false, null);
            assertEquals(0, ((Number) raw(m, "a").get("counter_value")).intValue(), "the counter was 1");
            m.dec(Map.of(F.counterValue, 1), m.createQueryFor(Audit.class).f("_id").eq("b"), false, false, null);
            assertEquals(1, ((Number) raw(m, "b").get("counter_value")).intValue(), "the counter was 2");
            assertFalse(hasJavaNames(raw(m, "b")), "stored under the stored name");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void decKeepsTheNumberTypeOfTheAmount(Morphium m) throws Exception {
        try (m) {
            setup(m, false);
            Query<Audit> q = m.createQueryFor(Audit.class).f("_id").eq("a");
            q.dec("counterValue", Integer.valueOf(1));
            assertTrue(raw(m, "a").get("counter_value") instanceof Integer, "an Integer amount keeps an int field an int: " + raw(m, "a").get("counter_value").getClass());
            assertEquals(0, ((Number) raw(m, "a").get("counter_value")).intValue());
            q.dec("counterValue", Long.valueOf(1));
            assertTrue(raw(m, "a").get("counter_value") instanceof Long, "a Long amount makes it a long, as inc does");
            Audit a = m.createQueryFor(Audit.class).f("_id").eq("a").get();
            m.dec(a, "counterValue", (Number) Integer.valueOf(1));
            assertTrue(raw(m, "a").get("counter_value") instanceof Long, "the long stays a long (-1 - 1)");
            q.dec("counterValue", Double.valueOf(0.5));
            assertEquals(-2.5, ((Number) raw(m, "a").get("counter_value")).doubleValue());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theAuditCoversEveryPathOnce(Morphium m) throws Exception {
        try (m) {
            Set<String> names = new java.util.TreeSet<>();
            PROBES.forEach(p -> names.add(p.name()));
            assertEquals(PROBES.size(), names.size(), "unique probe names");
            assertTrue(names.containsAll(SILENT_UNTIL_MOR_059_IS_DONE));
            
            assertTrue(names.containsAll(FAILING.keySet().stream().filter(n -> !n.equals("Morphium.currentDate")).toList()), "every failing probe has a translation probe");
        }
    }
}
