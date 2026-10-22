package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.EnumType;
import de.caluga.morphium.annotations.Enumerated;
import de.caluga.morphium.annotations.Id;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * MOR-093: an enum is stored by its name unless it is asked for otherwise: {@code @Enumerated(EnumType.ORDINAL)} on a field, or the setting {@code objectMappingSettings().setEnumType(ORDINAL)} for all
 * fields without the annotation. Stored as its ordinal an enum sorts and compares in the order of its constants. Reading takes both forms, whatever is configured.
 */
@Tag("core")
public class EnumStorageTest extends MultiDriverTestBase {

    public enum Level { LOW, MID, HIGH }

    @Entity
    public static class Task {
        @Id
        public String id;
        public Level plain;
        @Enumerated(EnumType.ORDINAL)
        public Level ordinal;
        @Enumerated(EnumType.STRING)
        public Level named;
        public List<Level> plainList;
        @Enumerated(EnumType.ORDINAL)
        public List<Level> ordinalList;
        @Enumerated(EnumType.ORDINAL)
        public Map<String, Level> ordinalMap;
    }

    private static Task task(String id, Level level) {
        Task t = new Task();
        t.id = id;
        t.plain = level;
        t.ordinal = level;
        t.named = level;
        t.plainList = new ArrayList<>(List.of(level));
        t.ordinalList = new ArrayList<>(List.of(level));
        t.ordinalMap = new java.util.LinkedHashMap<>(Map.of("k", level));
        return t;
    }

    /** the ids of the result, in alphabetical order */
    private static List<String> ids(Morphium m, de.caluga.morphium.query.Query<Task> query) {
        return query.asList().stream().map(t -> t.id).sorted().toList();
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aFieldIsStoredByNameUnlessAskedForOrdinal(Morphium m) throws Exception {
        try (m) {
            Map<String, Object> doc = m.getMapper().serialize(task("a", Level.HIGH));

            assertEquals("HIGH", doc.get("plain"));
            assertEquals(2, ((Number) doc.get("ordinal")).intValue());
            assertEquals("HIGH", doc.get("named"));
            assertEquals(List.of("HIGH"), doc.get("plain_list"));
            assertEquals(2, ((Number) ((List<?>) doc.get("ordinal_list")).get(0)).intValue());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theSettingChangesTheFieldsWithoutTheAnnotation(Morphium m) throws Exception {
        try (m) {
            m.getConfig().objectMappingSettings().setEnumType(EnumType.ORDINAL);

            Map<String, Object> doc = m.getMapper().serialize(task("a", Level.MID));

            assertEquals(1, ((Number) doc.get("plain")).intValue());
            assertEquals(1, ((Number) doc.get("ordinal")).intValue());
            assertEquals("MID", doc.get("named"), "the annotation wins over the setting");
            assertEquals(1, ((Number) ((List<?>) doc.get("plain_list")).get(0)).intValue());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void everythingIsReadBackInEveryMode(Morphium m) throws Exception {
        try (m) {
            for (EnumType mode : EnumType.values()) {
                m.getConfig().objectMappingSettings().setEnumType(mode);
                m.clearCollection(Task.class);
                m.store(task("a", Level.HIGH));

                m.getConfig().objectMappingSettings().setEnumType(mode == EnumType.STRING ? EnumType.ORDINAL : EnumType.STRING);
                Task back = m.createQueryFor(Task.class).f("id").eq("a").get();

                assertEquals(Level.HIGH, back.plain, mode + ": plain");
                assertEquals(Level.HIGH, back.ordinal, mode + ": ordinal");
                assertEquals(Level.HIGH, back.named, mode + ": named");
                assertEquals(List.of(Level.HIGH), back.plainList, mode + ": plainList");
                assertEquals(List.of(Level.HIGH), back.ordinalList, mode + ": ordinalList");
                assertEquals(Map.of("k", Level.HIGH), back.ordinalMap, mode + ": ordinalMap");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void queriesFindTheStoredForm(Morphium m) throws Exception {
        try (m) {
            m.getConfig().objectMappingSettings().setEnumType(EnumType.ORDINAL);
            m.store(task("low", Level.LOW));
            m.store(task("mid", Level.MID));
            m.store(task("high", Level.HIGH));

            for (String field : List.of("plain", "ordinal", "named")) {
                assertEquals(List.of("mid"), ids(m, m.createQueryFor(Task.class).f(field).eq(Level.MID)), field + " eq");
                assertEquals(List.of("high", "low"), ids(m, m.createQueryFor(Task.class).f(field).ne(Level.MID)), field + " ne");
                assertEquals(List.of("low", "mid"), ids(m, m.createQueryFor(Task.class).f(field).in(List.of(Level.LOW, Level.MID))), field + " in");
                assertEquals(List.of("high"), ids(m, m.createQueryFor(Task.class).f(field).nin(List.of(Level.LOW, Level.MID))), field + " nin");
            }

            // a comparison follows the order of the constants for an ordinal field
            assertEquals(List.of("high", "mid"), ids(m, m.createQueryFor(Task.class).f("ordinal").gt(Level.LOW)));
            assertEquals(List.of("low", "mid"), ids(m, m.createQueryFor(Task.class).f("plain").lte(Level.MID)));
            assertEquals(List.of("mid"), ids(m, m.createQueryFor(Task.class).f("ordinalList").eq(Level.MID)));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void sortingFollowsTheOrderOfTheConstants(Morphium m) throws Exception {
        try (m) {
            m.store(task("mid", Level.MID));
            m.store(task("high", Level.HIGH));
            m.store(task("low", Level.LOW));

            assertEquals(List.of("low", "mid", "high"), m.createQueryFor(Task.class).sort(Map.of("ordinal", 1)).asList().stream().map(t -> t.id).toList());
            assertEquals(List.of("high", "low", "mid"), m.createQueryFor(Task.class).sort(Map.of("named", 1)).asList().stream().map(t -> t.id).toList(), "by name: alphabetical");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void updatesWriteTheStoredForm(Morphium m) throws Exception {
        try (m) {
            m.store(task("a", Level.LOW));

            m.createQueryFor(Task.class).f("id").eq("a").modify().set("ordinal", Level.HIGH).set("named", Level.MID).push("ordinalList", Level.MID).updateOne();

            Task back = m.createQueryFor(Task.class).f("id").eq("a").get();
            assertEquals(Level.HIGH, back.ordinal);
            assertEquals(Level.MID, back.named);
            assertEquals(List.of(Level.LOW, Level.MID), back.ordinalList);
            assertEquals(1, m.createQueryFor(Task.class).f("ordinal").eq(Level.HIGH).countAll());
            assertEquals(1, m.createQueryFor(Task.class).f("named").eq(Level.MID).countAll());

            m.getConfig().objectMappingSettings().setEnumType(EnumType.ORDINAL);
            m.createQueryFor(Task.class).f("id").eq("a").modify().set("plain", Level.HIGH).updateOne();
            assertEquals(1, m.createQueryFor(Task.class).f("plain").eq(Level.HIGH).countAll());
            assertEquals(Level.HIGH, m.createQueryFor(Task.class).f("id").eq("a").get().plain);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anOrdinalThatIsNoConstantIsRefused(Morphium m) throws Exception {
        try (m) {
            m.store(task("a", Level.LOW));
            m.createQueryFor(Task.class).f("id").eq("a").set(Map.of("ordinal", 7), false, false);

            RuntimeException e = assertThrows(RuntimeException.class, () -> m.createQueryFor(Task.class).f("id").eq("a").get());
            Throwable cause = e;

            while (cause.getCause() != null) {
                cause = cause.getCause();
            }

            assertEquals(true, cause.getMessage().contains("ordinal") && cause.getMessage().contains("7"), cause.getMessage());
        }
    }
}
