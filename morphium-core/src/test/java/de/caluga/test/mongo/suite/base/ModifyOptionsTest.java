package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Collation;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.FindAndModifyOptions;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.query.ReturnDocument;
import de.caluga.morphium.query.UpdateOutcome;
import de.caluga.morphium.query.UpdateOutcome.Kind;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Child;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Lease;
import de.caluga.test.mongo.suite.base.ModifyFixtures.Plain;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static de.caluga.test.mongo.suite.base.ModifyFixtures.raw;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/** MOR-008: {@link FindAndModifyOptions} for the raw map path and the update builder, on a real MongoDB and on the InMemoryDriver. */
@Tag("core")
public class ModifyOptionsTest extends MultiDriverTestBase {

    private void seed(Morphium m, String id, String key, String payload, int count) {
        Plain p = new Plain();
        p.id = id;
        p.key = key;
        p.payload = payload;
        p.count = count;
        m.store(p);
    }

    private Query<Plain> byKey(Morphium m, String key) {
        return m.createQueryFor(Plain.class).f("key").eq(key);
    }

    private static Map<String, Object> set(String field, Object value) {
        return Doc.of("$set", Doc.of(field, value));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anUpsertThatInsertsReturnsTheDocumentForAfterAndNullForBefore(Morphium m) throws Exception {
        try (m) {
            Plain after = byKey(m, "k1").findOneAndUpdate(set("payload", "x"), FindAndModifyOptions.options().upsert(true).returnDocument(ReturnDocument.AFTER));
            assertNotNull(after);
            assertEquals("x", after.payload);
            Plain before = byKey(m, "k2").findOneAndUpdate(set("payload", "y"), FindAndModifyOptions.options().upsert(true).returnDocument(ReturnDocument.BEFORE));
            assertNull(before, "an insert has no pre-image: BEFORE returns null, as on every MongoDB");
            assertEquals("y", byKey(m, "k2").get().payload, "but the document was inserted");
            assertEquals(2, m.createQueryFor(Plain.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void beforeReturnsTheStateBeforeAndAfterTheStateAfter(Morphium m) throws Exception {
        try (m) {
            seed(m, "a", "k", "old", 1);
            assertEquals("old", byKey(m, "k").findOneAndUpdate(set("payload", "new"), FindAndModifyOptions.options().returnDocument(ReturnDocument.BEFORE)).payload);
            assertEquals("newer", byKey(m, "k").findOneAndUpdate(set("payload", "newer"), FindAndModifyOptions.options().returnDocument(ReturnDocument.AFTER)).payload);
            assertNull(byKey(m, "none").findOneAndUpdate(set("payload", "x"), FindAndModifyOptions.options().returnDocument(ReturnDocument.AFTER)), "no match, no upsert");
            assertEquals(1, m.createQueryFor(Plain.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theReturnDocumentIsRequiredAndNothingIsSentWithout(Morphium m) throws Exception {
        try (m) {
            seed(m, "a", "k", "old", 1);
            assertThrows(IllegalArgumentException.class, () -> byKey(m, "k").findOneAndUpdate(set("payload", "new"), FindAndModifyOptions.options()));
            assertThrows(IllegalArgumentException.class, () -> byKey(m, "k").findOneAndUpdate(set("payload", "new"), null));
            assertEquals("old", byKey(m, "k").get().payload);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theLegacyOverloadEqualsTheOptionsForm(Morphium m) throws Exception {
        try (m) {
            Plain legacy = byKey(m, "k").findOneAndUpdate(set("payload", "x"), true, true);
            Plain options = byKey(m, "k").findOneAndUpdate(set("payload", "y"), FindAndModifyOptions.options().upsert(true).returnDocument(ReturnDocument.AFTER));
            assertEquals(legacy.id, options.id, "the second call updates the document that the first one inserted");
            assertEquals("y", options.payload);
            assertEquals(1, m.createQueryFor(Plain.class).countAll());
            assertNull(byKey(m, "k2").findOneAndUpdate(set("payload", "z"), true, false), "legacy: returnNew=false on an insert is null, unchanged");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theSortOfTheOptionsPicksTheDocumentAndOverridesTheQuerySort(Morphium m) throws Exception {
        try (m) {
            seed(m, "a", "k", null, 1);
            seed(m, "b", "k", null, 2);
            seed(m, "c", "k", null, 3);
            Plain highest = byKey(m, "k").sort("count").findOneAndUpdate(set("payload", "taken"), FindAndModifyOptions.options().sort(Doc.of("count", -1)).returnDocument(ReturnDocument.AFTER));
            assertEquals("c", highest.id, "the sort of the options wins over the sort of the query");
            Plain lowest = byKey(m, "k").sort("count").findOneAndUpdate(set("payload", "taken"), FindAndModifyOptions.options().returnDocument(ReturnDocument.AFTER));
            assertEquals("a", lowest.id, "without an option the sort of the query applies, as before");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aProjectionReturnsOnlyTheRequestedFields(Morphium m) throws Exception {
        try (m) {
            seed(m, "a", "k", "payload", 7);
            Plain included = byKey(m, "k").findOneAndUpdate(set("count", 8), FindAndModifyOptions.options().projection(Doc.of("key", 1)).returnDocument(ReturnDocument.AFTER));
            assertEquals("a", included.id, "_id is kept");
            assertEquals("k", included.key);
            assertNull(included.payload);
            assertEquals(0, included.count);
            Plain excluded = byKey(m, "k").findOneAndUpdate(set("count", 9), FindAndModifyOptions.options().projection(Doc.of("payload", 0)).returnDocument(ReturnDocument.AFTER));
            assertEquals("k", excluded.key);
            assertNull(excluded.payload);
            assertEquals(9, excluded.count);
            assertEquals("payload", byKey(m, "k").get().payload, "the stored document is complete, only the result is projected");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void projectionKeysAreTranslatedToTheStoredNames(Morphium m) throws Exception {
        try (m) {
            Lease l = new Lease();
            l.id = "job";
            l.ownerName = "A";
            l.retryCount = 3;
            m.store(l);
            Lease r = m.createQueryFor(Lease.class).f("id").eq("job").findOneAndUpdate(set("retryCount", 4), FindAndModifyOptions.options().projection(Doc.of("ownerName", 1)).returnDocument(ReturnDocument.AFTER));
            assertEquals("A", r.ownerName, "ownerName is projected as owner_name");
            assertEquals(0, r.retryCount);
            assertEquals(4, m.createQueryFor(Lease.class).f("id").eq("job").get().retryCount);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aHintByNameOrByKeyDocumentIsAccepted(Morphium m) throws Exception {
        try (m) {
            seed(m, "a", "k", "x", 1);
            assertNotNull(byKey(m, "k").findOneAndUpdate(set("count", 2), FindAndModifyOptions.options().hint(Doc.of("_id", 1)).returnDocument(ReturnDocument.AFTER)));
            assertNotNull(byKey(m, "k").findOneAndUpdate(set("count", 3), FindAndModifyOptions.options().hint("_id_").returnDocument(ReturnDocument.AFTER)));
            assertEquals(3, byKey(m, "k").get().count);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anUnknownHintIsRejectedByAMongoDB(Morphium m) throws Exception {
        try (m) {
            assumeFalse(m.getDriver() instanceof InMemoryDriver, "the InMemoryDriver has no query planner: it accepts and ignores a hint");
            seed(m, "a", "k", "x", 1);
            assertThrows(MorphiumDriverException.class, () -> byKey(m, "k").findOneAndUpdate(set("count", 2), FindAndModifyOptions.options().hint("no_such_index").returnDocument(ReturnDocument.AFTER)));
            assertEquals(1, byKey(m, "k").get().count);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aMaxTimeIsAccepted(Morphium m) throws Exception {
        try (m) {
            seed(m, "a", "k", "x", 1);
            assertEquals(2, byKey(m, "k").findOneAndUpdate(set("count", 2), FindAndModifyOptions.options().maxTime(Duration.ofSeconds(30)).returnDocument(ReturnDocument.AFTER)).count);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void arrayFiltersOfTheOptionsAreApplied(Morphium m) throws Exception {
        try (m) {
            Lease l = new Lease();
            l.id = "j";
            l.children = new ArrayList<>(List.of(new Child("x"), new Child("y")));
            m.store(l);
            Lease r = m.createQueryFor(Lease.class).f("id").eq("j").findOneAndUpdate(set("children.$[c].file_name", "Y"),
                      FindAndModifyOptions.options().arrayFilters(Doc.of("c.file_name", "y")).returnDocument(ReturnDocument.AFTER));
            assertEquals(List.of("x", "Y"), r.children.stream().map(c -> c.fileName).toList());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aCollationOfTheOptionsIsApplied(Morphium m) throws Exception {
        try (m) {
            assumeFalse(m.getDriver() instanceof InMemoryDriver, "collations do not work with the InMemoryDriver");
            seed(m, "a", "ABC", "x", 1);
            Collation caseInsensitive = new Collation().locale("en").strength(Collation.Strength.SECONDARY);
            assertNull(byKey(m, "abc").findOneAndUpdate(set("count", 2), FindAndModifyOptions.options().returnDocument(ReturnDocument.AFTER)), "without a collation the case differs");
            Plain r = byKey(m, "abc").findOneAndUpdate(set("count", 2), FindAndModifyOptions.options().collation(caseInsensitive).returnDocument(ReturnDocument.AFTER));
            assertNotNull(r);
            assertEquals(2, r.count);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theBuilderTakesTheOptions(Morphium m) throws Exception {
        try (m) {
            seed(m, "a", "k", "payload", 1);
            Plain r = byKey(m, "k").modify().inc("count", 1).findOneAndUpdate(FindAndModifyOptions.options().projection(Doc.of("count", 1)).hint(Doc.of("_id", 1)).returnDocument(ReturnDocument.AFTER));
            assertEquals(2, r.count);
            assertNull(r.payload, "projected");
            assertEquals("payload", byKey(m, "k").get().payload);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theBuilderUpsertAndTheOptionUpsertAreCombined(Morphium m) throws Exception {
        try (m) {
            Plain viaBuilder = byKey(m, "k1").modify().set("payload", "x").upsert().findOneAndUpdate(FindAndModifyOptions.options().upsert(false).returnDocument(ReturnDocument.AFTER));
            assertNotNull(viaBuilder, "upsert() of the builder cannot be switched off by an option");
            Plain viaOption = byKey(m, "k2").modify().set("payload", "x").findOneAndUpdate(FindAndModifyOptions.options().upsert(true).returnDocument(ReturnDocument.AFTER));
            assertNotNull(viaOption, "the option can request the upsert");
            assertEquals(2, m.createQueryFor(Plain.class).countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void tryFindOneAndUpdateTakesTheOptionsAndTellsTheInsert(Morphium m) throws Exception {
        try (m) {
            UpdateOutcome<Plain> inserted = byKey(m, "k").modify().set("payload", "x").upsert().tryFindOneAndUpdate(FindAndModifyOptions.options().projection(Doc.of("key", 1)).returnDocument(ReturnDocument.BEFORE));
            assertEquals(Kind.INSERTED, inserted.getKind());
            assertNull(inserted.getDocument());
            UpdateOutcome<Plain> updated = byKey(m, "k").modify().set("payload", "y").tryFindOneAndUpdate(FindAndModifyOptions.options().projection(Doc.of("key", 1)).returnDocument(ReturnDocument.BEFORE));
            assertEquals(Kind.UPDATED, updated.getKind());
            assertEquals("k", updated.getDocument().key);
            assertNull(updated.getDocument().payload);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theBuilderTerminalsKeepTheirValidation(Morphium m) throws Exception {
        try (m) {
            seed(m, "a", "k", "x", 1);
            FindAndModifyOptions o = FindAndModifyOptions.options().returnDocument(ReturnDocument.AFTER);
            assertThrows(IllegalStateException.class, () -> byKey(m, "k").modify().findOneAndUpdate(o), "an empty builder would replace the document");
            assertThrows(IllegalStateException.class, () -> byKey(m, "k").modify().setOnInsert("payload", "x").findOneAndUpdate(o), "setOnInsert without upsert");
            assertThrows(IllegalArgumentException.class, () -> byKey(m, "k").modify().set("payload", "x").findOneAndUpdate((FindAndModifyOptions) null));
            assertThrows(IllegalArgumentException.class, () -> byKey(m, "k").modify().set("payload", "x").findOneAndUpdate(FindAndModifyOptions.options()));
            assertEquals("x", byKey(m, "k").get().payload);
            assertFalse(raw(m, Plain.class, "a").containsKey("zz"));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void theCountTerminalsStillRejectProjectionAndHint(Morphium m) throws Exception {
        try (m) {
            seed(m, "a", "k", "x", 1);
            assertThrows(UnsupportedOperationException.class, () -> byKey(m, "k").setProjection("payload").modify().set("payload", "y").updateOne());
            assertThrows(UnsupportedOperationException.class, () -> byKey(m, "k").setHintString("_id_").modify().set("payload", "y").updateMany());
            assertTrue(true);
        }
    }
}
