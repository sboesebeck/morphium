package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.bson.BsonDecoder;
import de.caluga.morphium.driver.bson.BsonEncoder;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #336: whatever a driver stores for a value that reaches it UNMAPPED must have the shape a real
 * server would have given it. The wire drivers get that for free - every command is serialised by
 * {@link BsonEncoder} and read back through {@link BsonDecoder}, so the stored shape is
 * {@code decode(encode(v))} by construction. {@code InMemoryDriver} has no encoder in that path
 * and stores the Java object verbatim, so it holds a {@code LocalDate} where mongod holds a
 * {@code Long}, a {@code Character} where mongod holds an {@code Integer}, and so on.
 *
 * <p>The reference is deliberately {@code decode(encode(v))} rather than a second, live driver:
 * it is the same invariant ("the shape the wire produces"), it pins the wire drivers just as
 * tightly, and it needs no running MongoDB - so the guard runs in the default in-memory suite,
 * which is exactly where the divergence has to be caught.
 *
 * <p>Why this bug class needs a RAW-shape test at all: it is invisible from query results. The
 * in-memory driver is internally consistent - it leaves both the stored value and the filter
 * unnormalised, so equality still matches and a format test asserting on query outcomes passes
 * meaninglessly. Only the persisted shape shows the difference.
 *
 * <p>Disabled until the normalisation lands (6.4.0, see #336). It fails on {@code InMemoryDriver}
 * today, by design - that failure IS the issue. Enable it together with the fix; it then doubles
 * as the drift guard for the object-space {@code normalize()} against {@link BsonEncoder}'s
 * type table.
 */
@Tag("core")
@Disabled("#336: InMemoryDriver stores unmapped values verbatim where the wire path normalises "
          + "them - verified red on InMemDriver / green on a real mongod. Enable with the "
          + "normalisation fix (6.4.0).")
public class InMemoryWireShapeParityTest extends MultiDriverTestBase {

    private static final String COLL = "issue336_shape_parity";

    /** Values that reach a driver unmapped and are normalised by BsonEncoder on the wire path. */
    private static Map<String, Object> unmappedValues() {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("localDate", LocalDate.ofEpochDay(20689L));
        v.put("localDateTime", LocalDateTime.of(2026, 8, 25, 21, 43, 24));
        v.put("localTime", LocalTime.of(21, 43, 24));
        v.put("instant", Instant.ofEpochSecond(1600000000L, 123456789));
        v.put("character", 'A');
        v.put("anEnum", Shape.ROUND);
        v.put("aShort", (short) 7);
        v.put("aByte", (byte) 3);
        v.put("aFloat", 1.5f);
        v.put("intArray", new int[]{1, 2});
        v.put("calendar", fixedCalendar());
        v.put("objectId", new org.bson.types.ObjectId("64f0000000000000000000ab"));
        // NOT a divergence this guard can see: the wire stores Decimal128, but BsonDecoder maps
        // it straight back to BigDecimal, so both sides read back the same class. The on-disk
        // BSON type still differs ($type:"decimal" vs. a Java object) - kept here as a pin, so a
        // future normalisation that turns BigDecimal into something else fails loudly.
        v.put("bigDecimal", new BigDecimal("2.500"));
        return v;
    }

    public enum Shape { ROUND, SQUARE }

    private static Calendar fixedCalendar() {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        c.setTimeInMillis(1600000000000L);
        return c;
    }

    /**
     * A document inserted through the driver API must come back in the shape the wire produces -
     * per field, so a failure names the type that diverges instead of dumping the whole document.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void insertStoresWireShape(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumId id = new MorphiumId();
            Map<String, Object> doc = new LinkedHashMap<>(unmappedValues());
            doc.put("_id", id);
            insertRaw(morphium, doc);

            Map<String, Object> stored = findRaw(morphium, id);
            assertNotNull(stored, "inserted document not found");
            assertWireShape(morphium, stored, unmappedValues());
        }
    }

    /**
     * Same for the update path: {@code $set} operands travel through the encoder on the wire just
     * like inserted documents do, so what they leave behind has to match.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void setStoresWireShape(Morphium morphium) throws Exception {
        try (morphium) {
            MorphiumId id = new MorphiumId();
            Map<String, Object> seed = new LinkedHashMap<>();
            seed.put("_id", id);
            insertRaw(morphium, seed);

            UpdateMongoCommand upd = new UpdateMongoCommand(morphium.getDriver().getPrimaryConnection(null))
                    .setDb(morphium.getDatabase()).setColl(COLL);
            try {
                upd.addUpdate(Doc.of("_id", id), Doc.of("$set", new LinkedHashMap<>(unmappedValues())),
                        null, false, false, null, null, null);
                upd.execute();
            } finally {
                upd.releaseConnection();
            }

            Map<String, Object> stored = findRaw(morphium, id);
            assertNotNull(stored, "updated document not found");
            assertWireShape(morphium, stored, unmappedValues());
        }
    }

    /** Asserts every field holds what {@code decode(encode(value))} produces for it. */
    private static void assertWireShape(Morphium morphium, Map<String, Object> stored,
                                        Map<String, Object> written) throws Exception {
        String drv = morphium.getDriver().getName();
        List<String> mismatches = new ArrayList<>();

        for (Map.Entry<String, Object> e : written.entrySet()) {
            Object expected = wireShapeOf(e.getKey(), e.getValue());
            Object actual = stored.get(e.getKey());

            if (!shapesEqual(expected, actual)) {
                mismatches.add(String.format("  %-14s written %-38s expected %-30s but stored %s",
                        e.getKey(), describe(e.getValue()), describe(expected), describe(actual)));
            }
        }

        assertTrue(mismatches.isEmpty(), () -> "[" + drv + "] stored shape differs from what the "
                + "wire path (BsonEncoder/BsonDecoder) produces for the same value - see #336:"
                + System.lineSeparator() + String.join(System.lineSeparator(), mismatches));
    }

    /** What a real server would hold for {@code value}: one BSON round trip, nothing else. */
    private static Object wireShapeOf(String field, Object value) throws Exception {
        return BsonDecoder.decodeDocument(BsonEncoder.encodeDocument(Doc.of(field, value))).get(field);
    }

    /** Type plus value, short enough to stay readable in a failure listing (Calendar prints ~700 chars). */
    private static String describe(Object o) {
        if (o == null) {
            return "null";
        }
        String value = o.getClass().isArray()
                ? java.util.Arrays.deepToString(new Object[]{o}).replaceAll("^\\[|\\]$", "")
                : String.valueOf(o);
        if (value.length() > 48) {
            value = value.substring(0, 45) + "...";
        }
        return o.getClass().getSimpleName() + "(" + value + ")";
    }

    /** Structural equality; Integer/Long width differences between drivers are not the point here. */
    private static boolean shapesEqual(Object a, Object b) {
        if (a == null || b == null) {
            return a == b;
        }
        if (a instanceof Number && b instanceof Number) {
            return a.getClass().equals(b.getClass())
                   && ((Number) a).doubleValue() == ((Number) b).doubleValue();
        }
        if (a instanceof Map<?, ?> ma && b instanceof Map<?, ?> mb) {
            if (ma.size() != mb.size()) {
                return false;
            }
            for (Map.Entry<?, ?> e : ma.entrySet()) {
                if (!mb.containsKey(e.getKey()) || !shapesEqual(e.getValue(), mb.get(e.getKey()))) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof List<?> la && b instanceof List<?> lb) {
            if (la.size() != lb.size()) {
                return false;
            }
            for (int i = 0; i < la.size(); i++) {
                if (!shapesEqual(la.get(i), lb.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return a.getClass().equals(b.getClass()) && a.equals(b);
    }

    private static void insertRaw(Morphium morphium, Map<String, Object> doc) throws Exception {
        InsertMongoCommand cmd = new InsertMongoCommand(morphium.getDriver().getPrimaryConnection(null))
                .setDb(morphium.getDatabase()).setColl(COLL).setDocuments(List.of(doc));
        try {
            cmd.execute();
        } finally {
            cmd.releaseConnection();
        }
    }

    private static Map<String, Object> findRaw(Morphium morphium, MorphiumId id) throws Exception {
        FindCommand fnd = new FindCommand(morphium.getDriver().getPrimaryConnection(null))
                .setDb(morphium.getDatabase()).setColl(COLL).setFilter(Doc.of("_id", id));
        try {
            List<Map<String, Object>> res = fnd.execute();
            return res.isEmpty() ? null : res.get(0);
        } finally {
            fnd.releaseConnection();
        }
    }
}
