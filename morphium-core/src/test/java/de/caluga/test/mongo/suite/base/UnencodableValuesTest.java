package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.FindAndModifyMongoCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.StoreMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-029: a command that carries a value the wire encoder cannot encode (a POJO as a single value, in a List, a Set, an array or a Map) fails the same way on every driver: a
 * {@link MorphiumDriverException} "Error sending Request: " whose cause is the {@code RuntimeException "Unhandled Data type: ..."}, before anything is written. The InMemoryDriver
 * used to store the live Java objects, or to fail with a ClassCastException after a partial write (hypothesis 4 of the research).
 */
@Tag("core")
public class UnencodableValuesTest extends MultiDriverTestBase {
    private static final AtomicInteger COUNTER = new AtomicInteger();

    public static class Pojo {
        public String name = "a";
    }

    @FunctionalInterface
    private interface Action {
        void run(MongoConnection con) throws Exception;
    }

    private static Map<String, Object> shapes() {
        Map<String, Object> shapes = new LinkedHashMap<>();
        shapes.put("single", new Pojo());
        shapes.put("list", new ArrayList<>(List.of(new Pojo(), new Pojo())));
        shapes.put("set", new LinkedHashSet<>(List.of(new Pojo())));
        shapes.put("array", new Pojo[] {new Pojo()});
        shapes.put("map", new LinkedHashMap<>(Map.of("x", new Pojo())));
        shapes.put("mapInMap", new LinkedHashMap<>(Map.of("outer", new LinkedHashMap<>(Map.of("x", new Pojo())))));
        return shapes;
    }

    private static void with(Morphium m, Action action) throws Exception {
        MongoConnection con = m.getDriver().getPrimaryConnection(null);

        try {
            action.run(con);
        } finally {
            m.getDriver().releaseConnection(con);
        }
    }

    /** a collection of its own with the document {_id: 1, n: 1} */
    private static String fresh(Morphium m) throws Exception {
        String coll = "unencodable_" + COUNTER.incrementAndGet() + "_" + System.nanoTime();
        with(m, con -> new InsertMongoCommand(con).setDb(m.getDatabase()).setColl(coll).setDocuments(new ArrayList<>(List.of(new LinkedHashMap<>(Doc.of("_id", 1, "n", 1))))).execute());
        return coll;
    }

    private static List<Map<String, Object>> all(Morphium m, String coll) throws Exception {
        List<Map<String, Object>> result = new ArrayList<>();
        with(m, con -> {
            FindCommand find = new FindCommand(con).setDb(m.getDatabase()).setColl(coll).setFilter(Map.of());
            result.addAll(find.execute());
        });
        return result;
    }

    private static void assertUnencodable(Throwable t) {
        assertTrue(t instanceof MorphiumDriverException, String.valueOf(t));
        assertTrue(t.getMessage().startsWith("Error sending Request"), t.getMessage());
        Throwable cause = t.getCause();
        assertNotNull(cause, "the cause of the refusal");
        assertTrue(cause instanceof RuntimeException, String.valueOf(cause));
        assertTrue(cause.getMessage().startsWith("Unhandled Data type: "), cause.getMessage());
        assertTrue(cause.getMessage().contains(Pojo.class.getName()), cause.getMessage());
    }

    private static void assertUnchanged(Morphium m, String coll) throws Exception {
        List<Map<String, Object>> docs = all(m, coll);
        assertEquals(1, docs.size(), String.valueOf(docs));
        assertEquals(Map.of("_id", 1, "n", 1), new LinkedHashMap<>(docs.get(0)), "the document is unchanged");
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anUpdateCommandWithAPojoFailsLikeAWireDriverAndChangesNothing(Morphium m) throws Exception {
        try (m) {
            for (var shape : shapes().entrySet()) {
                String coll = fresh(m);
                Exception e = assertThrows(Exception.class, () -> with(m, con -> {
                    UpdateMongoCommand cmd = new UpdateMongoCommand(con).setDb(m.getDatabase()).setColl(coll);
                    cmd.addUpdate(Doc.of("_id", 1), Doc.of("$set", Doc.of("f", shape.getValue())), null, false, false, null, null, null);
                    cmd.execute();
                }), shape.getKey());
                assertUnencodable(e);
                assertUnchanged(m, coll);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anUpdateOperatorOtherThanSetIsCheckedToo(Morphium m) throws Exception {
        try (m) {
            for (String op : List.of("$push", "$addToSet", "$setOnInsert")) {
                String coll = fresh(m);
                Exception e = assertThrows(Exception.class, () -> with(m, con -> {
                    UpdateMongoCommand cmd = new UpdateMongoCommand(con).setDb(m.getDatabase()).setColl(coll);
                    cmd.addUpdate(Doc.of("_id", 1), Doc.of(op, Doc.of("f", new Pojo())), null, op.equals("$setOnInsert"), false, null, null, null);
                    cmd.execute();
                }), op);
                assertUnencodable(e);
                assertUnchanged(m, coll);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aFindAndModifyWithAPojoFailsLikeAWireDriverAndChangesNothing(Morphium m) throws Exception {
        try (m) {
            for (var shape : shapes().entrySet()) {
                String coll = fresh(m);
                Exception e = assertThrows(Exception.class, () -> with(m, con -> new FindAndModifyMongoCommand(con).setDb(m.getDatabase()).setColl(coll).setQuery(Doc.of("_id", 1))
                        .setUpdate(Doc.of("$set", Doc.of("f", shape.getValue()))).setNewFlag(true).execute()), shape.getKey());
                assertUnencodable(e);
                assertUnchanged(m, coll);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anInsertWithAPojoFailsLikeAWireDriverAndInsertsNothing(Morphium m) throws Exception {
        try (m) {
            for (var shape : shapes().entrySet()) {
                String coll = fresh(m);
                Exception e = assertThrows(Exception.class, () -> with(m, con -> new InsertMongoCommand(con).setDb(m.getDatabase()).setColl(coll)
                        .setDocuments(new ArrayList<>(List.of(new LinkedHashMap<>(Doc.of("_id", 2, "f", shape.getValue()))))).execute()), shape.getKey());
                assertUnencodable(e);
                assertUnchanged(m, coll);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aStoreWithAPojoFailsLikeAWireDriverAndStoresNothing(Morphium m) throws Exception {
        try (m) {
            String coll = fresh(m);
            Exception e = assertThrows(Exception.class, () -> with(m, con -> new StoreMongoCommand(con).setDb(m.getDatabase()).setColl(coll)
                    .setDocuments(new ArrayList<>(List.of(new LinkedHashMap<>(Doc.of("_id", 2, "f", new Pojo()))))).execute()));
            assertUnencodable(e);
            assertUnchanged(m, coll);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void whatTheEncoderTakesIsAcceptedOnEveryDriver(Morphium m) throws Exception {
        try (m) {
            String coll = fresh(m);
            Set<String> flags = new LinkedHashSet<>(List.of("x", "y"));
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("set", flags);
            value.put("day", LocalDate.of(2026, 1, 2));
            value.put("date", new Date());
            value.put("uuid", UUID.randomUUID());
            value.put("pattern", Pattern.compile("a.*"));
            value.put("bytes", new byte[] {1, 2});
            value.put("nested", List.of(Doc.of("k", 1)));
            with(m, con -> {
                UpdateMongoCommand cmd = new UpdateMongoCommand(con).setDb(m.getDatabase()).setColl(coll);
                cmd.addUpdate(Doc.of("_id", 1), Doc.of("$set", Doc.of("f", value)), null, false, false, null, null, null);
                cmd.execute();
            });
            assertEquals(1, all(m, coll).size());
            assertNotNull(all(m, coll).get(0).get("f"));
        }
    }
}
