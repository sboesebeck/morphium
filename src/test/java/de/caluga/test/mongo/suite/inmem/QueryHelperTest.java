package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.MongoType;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.QueryHelper;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.base.GeoSearchTests;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.ListContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("inmemory")
public class QueryHelperTest extends MorphiumInMemTestBase {

    @Test
    public void typeTest() {
        UncachedObject uc = new UncachedObject("strVal", 12);
        uc.setDval(12.0);
        uc.setMorphiumId(new MorphiumId());
        uc.setBinaryData(new byte[] {1, 2, 3, 4});
        uc.setLongData(new long[] {12L, 10202L});
        uc.setFloatData(new float[] {12.0f, 122f});

        var query = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.morphiumId).type(MongoType.OBJECT_ID).toQueryObject();
        assertTrue(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(uc), null));

        query = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.morphiumId).type(MongoType.INTEGER).toQueryObject();
        assertFalse(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(uc), null));

        query = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).type(MongoType.INTEGER).toQueryObject();
        assertTrue(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(uc), null));

        query = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).type(MongoType.STRING).toQueryObject();
        assertFalse(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(uc), null));

        query = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.binaryData).type(MongoType.BINARY_DATA).toQueryObject();
        assertTrue(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(uc), null));

        query = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.longData).type(MongoType.LONG).toQueryObject();
        assertTrue(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(uc), null));

        query = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.longData).type(MongoType.STRING).toQueryObject();
        assertFalse(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(uc), null));

        query = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.boolData).type(MongoType.NULL).toQueryObject();
        assertTrue(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(uc), null));

    }
    @Test
    public void geoSearchWithinBoxTest() throws Exception {
        GeoSearchTests.Place p = new GeoSearchTests.Place();
        List<Double> pos = new ArrayList<>();
        pos.add(100.0);
        pos.add(100.0);
        p.setName("P100");
        p.setPosition(pos);

        var query = morphium.createQueryFor(GeoSearchTests.Place.class).f(GeoSearchTests.Place.Fields.position).geoWithinBox(90.0, 90.0, 110.0, 110.0).toQueryObject();
        assertTrue(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(p), null));

        query = morphium.createQueryFor(GeoSearchTests.Place.class).f(GeoSearchTests.Place.Fields.position).geoWithinBox(110.0, 90.0, 130.0, 110.0).toQueryObject();
        assertFalse(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(p), null));

    }

    @Test
    public void elemAllTest() throws Exception {
        ListContainer c = new ListContainer();
        c.addString("test 1");
        c.addString("test 2");
        c.addString("test 3");
        Map<String, Object> doc = morphium.getMapper().serialize(c);
        Map<String, Object> query = morphium.createQueryFor(ListContainer.class).f(ListContainer.Fields.stringList).all("test 1", "test 2").toQueryObject();
        assertTrue(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(c), null));
        query = morphium.createQueryFor(ListContainer.class).f(ListContainer.Fields.stringList).all("test 1", "test not").toQueryObject();
        assertFalse(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(c), null));

        query = morphium.createQueryFor(ListContainer.class).f(ListContainer.Fields.stringList).size(3).toQueryObject();
        assertTrue(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(c), null));

        query = morphium.createQueryFor(ListContainer.class).f(ListContainer.Fields.stringList).size(1).toQueryObject();
        assertFalse(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(c), null));
    }

    @Test
    public void elemMatchTest() throws Exception {
        ListContainer c = new ListContainer();
        c.addString("test 1");
        c.addString("test 2");
        c.addString("test 3");

        Map<String, Object> doc = morphium.getMapper().serialize(c);
        Map<String, Object> query = morphium.createQueryFor(ListContainer.class).f(ListContainer.Fields.stringList).elemMatch(Doc.of("$eq", "test 1")).toQueryObject();
        assertTrue(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(c), null));

        query = morphium.createQueryFor(ListContainer.class).f(ListContainer.Fields.stringList).elemMatch(Doc.of("$eq", "test no")).toQueryObject();

        assertFalse(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(c), null));
        c = new ListContainer();
        c.addEmbedded(new EmbeddedObject("test1", "val1", 12));
        c.addEmbedded(new EmbeddedObject("test2", "val2", 42));
        c.addEmbedded(new EmbeddedObject("test3", "val3", 451));

        var embQ = morphium.createQueryFor(EmbeddedObject.class).f(EmbeddedObject.Fields.name).eq("test1");
        query = morphium.createQueryFor(ListContainer.class).f(ListContainer.Fields.embeddedObjectList).elemMatch(embQ).toQueryObject();
        assertTrue(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(c), null));

        embQ = morphium.createQueryFor(EmbeddedObject.class).f(EmbeddedObject.Fields.name).eq("test no");
        query = morphium.createQueryFor(ListContainer.class).f(ListContainer.Fields.embeddedObjectList).elemMatch(embQ).toQueryObject();
        assertFalse(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(c), null));

        embQ = morphium.createQueryFor(EmbeddedObject.class).f(EmbeddedObject.Fields.testValueLong).gt(42);
        query = morphium.createQueryFor(ListContainer.class).f(ListContainer.Fields.embeddedObjectList).elemMatch(embQ).toQueryObject();
        assertTrue(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(c), null));

        embQ = morphium.createQueryFor(EmbeddedObject.class).f(EmbeddedObject.Fields.testValueLong).gt(45).f(EmbeddedObject.Fields.testValueLong).lt(400);
        query = morphium.createQueryFor(ListContainer.class).f(ListContainer.Fields.embeddedObjectList).elemMatch(embQ).toQueryObject();
        assertFalse(QueryHelper.matchesQuery(query, morphium.getMapper().serialize(c), null));

        assertTrue(QueryHelper.matchesQuery(
            Doc.of("arr", Doc.of("$elemMatch", Doc.of("x", Doc.of("$gt", 2)))),
            Doc.of("arr", List.of(Doc.of("x", 3))),
            null
        ));
    }

    @Test
    public void bitMatchTest() throws Exception {
        Map<String, Object> doc = UtilsMap.of("counter", (Object) 12, "str_value", "hello");
        Map<String, Object> query = morphium.createQueryFor(UncachedObject.class).f("counter").bitsAllClear(0, 1).toQueryObject();
        assertTrue(QueryHelper.matchesQuery(query, doc, null));
        query = morphium.createQueryFor(UncachedObject.class).f("counter").bitsAllClear(0, 1, 2).toQueryObject();
        assertFalse(QueryHelper.matchesQuery(query, doc, null));

        query = morphium.createQueryFor(UncachedObject.class).f("counter").bitsAllSet(2, 3).toQueryObject();
        assertTrue(QueryHelper.matchesQuery(query, doc, null));
        query = morphium.createQueryFor(UncachedObject.class).f("counter").bitsAllSet(0, 2, 3).toQueryObject();
        assertFalse(QueryHelper.matchesQuery(query, doc, null));

        query = morphium.createQueryFor(UncachedObject.class).f("counter").bitsAnySet(0, 1, 2, 3).toQueryObject();
        assertTrue(QueryHelper.matchesQuery(query, doc, null));
        query = morphium.createQueryFor(UncachedObject.class).f("counter").bitsAnySet(0, 1).toQueryObject();
        assertFalse(QueryHelper.matchesQuery(query, doc, null));

        query = morphium.createQueryFor(UncachedObject.class).f("counter").bitsAnyClear(0, 1, 2, 3).toQueryObject();
        assertTrue(QueryHelper.matchesQuery(query, doc, null));
        query = morphium.createQueryFor(UncachedObject.class).f("counter").bitsAnyClear(2, 3).toQueryObject();
        assertFalse(QueryHelper.matchesQuery(query, doc, null));
    }

    @Test
    public void simpleMatchTest() throws Exception {
        Map<String, Object> doc = UtilsMap.of("counter", (Object) 12, "str_value", "hello");

        Map<String, Object> query = morphium.createQueryFor(UncachedObject.class).f("counter").eq(12)
         .f("str_value").eq("not hello").toQueryObject();
        assertFalse(QueryHelper.matchesQuery(query, doc, null));

        query = morphium.createQueryFor(UncachedObject.class).f("counter").eq(12)
         .f("strValue").eq("hello").toQueryObject();

        assertTrue(QueryHelper.matchesQuery(query, doc, null));

    }

    @Test
    public void referenceListSimpleEqTest() {
        UncachedObject referenced = new UncachedObject();
        referenced.setMorphiumId(new MorphiumId());
        ListContainer container = new ListContainer();
        container.addRef(referenced);

        Map<String, Object> serialized = morphium.getMapper().serialize(container);
        Map<String, Object> query = morphium.createQueryFor(ListContainer.class)
         .f(ListContainer.Fields.refList).eq(referenced).toQueryObject();

        assertTrue(query.containsKey("ref_list.refid"), () -> "Missing ref_list.refid key: " + query.keySet());
        assertTrue(query.get("ref_list.refid") instanceof ObjectId, () -> "Unexpected value type: " + query.get("ref_list.refid").getClass());

        assertTrue(QueryHelper.matchesQuery(query, serialized, null));
    }

    @Test
    public void referenceListQueryMatches() {
        morphium.dropCollection(ListContainer.class);
        morphium.dropCollection(UncachedObject.class);

        UncachedObject referenced = new UncachedObject();
        referenced.setStrValue("ref");
        referenced.setCounter(42);
        morphium.store(referenced);
        waitForWrites();

        ListContainer container = new ListContainer();
        container.addRef(referenced);
        morphium.store(container);
        waitForWrites();

        assertEquals(1, morphium.createQueryFor(ListContainer.class)
         .f("refList").eq(referenced).countAll());
    }

    @Test
    public void combinedRangeOperatorTest() {
        Map<String, Object> doc = UtilsMap.of("value", (Object) 10);

        Map<String, Object> query = Doc.of("value", Doc.of("$gte", 5, "$lte", 15));
        assertTrue(QueryHelper.matchesQuery(query, doc, null));

        query = Doc.of("value", Doc.of("$gte", 11, "$lte", 12));
        assertFalse(QueryHelper.matchesQuery(query, doc, null));
    }

    @Test
    public void regexOptionsTest() {
        Map<String, Object> doc = UtilsMap.of("text", (Object) "Hello\nWorld");

        Map<String, Object> query = Doc.of("text", Doc.of("$regex", "world", "$options", "i"));
        assertTrue(QueryHelper.matchesQuery(query, doc, null));

        query = Doc.of("text", Doc.of("$regex", "^world", "$options", "im"));
        assertTrue(QueryHelper.matchesQuery(query, doc, null));

        query = Doc.of("text", Doc.of("$regex", "^world"));
        assertFalse(QueryHelper.matchesQuery(query, doc, null));
    }

    @Test
    public void textSearchTokenizationTest() {
        Map<String, Object> doc = UtilsMap.of("description", (Object) "The quick brown fox jumps over the lazy dog");

        Map<String, Object> query = Doc.of("description", Doc.of("$text", "quick \"brown fox\""));
        assertTrue(QueryHelper.matchesQuery(query, doc, null));

        query = Doc.of("description", Doc.of("$text", "hedgehog"));
        assertFalse(QueryHelper.matchesQuery(query, doc, null));
    }

    @Test
    public void rootLevelTextSearchTest() {
        // Test MongoDB-compatible root-level $text query format
        // { $text: { $search: "search terms" } }

        // Create test documents
        Map<String, Object> doc1 = UtilsMap.of(
            "title", (Object) "The quick brown fox",
            "content", "jumps over the lazy dog"
        );
        Map<String, Object> doc2 = UtilsMap.of(
            "title", (Object) "A slow hedgehog",
            "content", "walks in the garden"
        );

        // Internal format for $textSearch (as transformed by InMemoryDriver)
        // This tests the QueryHelper.matchesTextSearch method directly
        Map<String, Object> textSearchQuery = Doc.of("$textSearch", Doc.of(
            "search", "quick fox",
            "fields", List.of("title", "content"),
            "caseSensitive", false
        ));

        assertTrue(QueryHelper.matchesQuery(textSearchQuery, doc1, null),
            "Should match doc1 with 'quick fox' in title");
        assertFalse(QueryHelper.matchesQuery(textSearchQuery, doc2, null),
            "Should not match doc2 - no 'quick' or 'fox'");

        // Test phrase search
        textSearchQuery = Doc.of("$textSearch", Doc.of(
            "search", "\"brown fox\"",
            "fields", List.of("title"),
            "caseSensitive", false
        ));
        assertTrue(QueryHelper.matchesQuery(textSearchQuery, doc1, null),
            "Should match phrase 'brown fox' in title");

        // Test negation
        textSearchQuery = Doc.of("$textSearch", Doc.of(
            "search", "quick -lazy",
            "fields", List.of("title", "content"),
            "caseSensitive", false
        ));
        assertFalse(QueryHelper.matchesQuery(textSearchQuery, doc1, null),
            "Should not match - has 'lazy' (negated)");

        // Test case sensitivity
        textSearchQuery = Doc.of("$textSearch", Doc.of(
            "search", "QUICK",
            "fields", List.of("title"),
            "caseSensitive", true
        ));
        assertFalse(QueryHelper.matchesQuery(textSearchQuery, doc1, null),
            "Case sensitive search for 'QUICK' should not match 'quick'");

        textSearchQuery = Doc.of("$textSearch", Doc.of(
            "search", "QUICK",
            "fields", List.of("title"),
            "caseSensitive", false
        ));
        assertTrue(QueryHelper.matchesQuery(textSearchQuery, doc1, null),
            "Case insensitive search for 'QUICK' should match 'quick'");

        // Test search across all fields (empty fields list)
        textSearchQuery = Doc.of("$textSearch", Doc.of(
            "search", "garden",
            "fields", List.of(),
            "caseSensitive", false
        ));
        assertTrue(QueryHelper.matchesQuery(textSearchQuery, doc2, null),
            "Should find 'garden' when searching all fields");
        assertFalse(QueryHelper.matchesQuery(textSearchQuery, doc1, null),
            "Should not find 'garden' in doc1");
    }

    @Test
    public void inOperatorMatchesObjectIds() {
        MorphiumId morphiumId = new MorphiumId();
        Map<String, Object> doc = UtilsMap.of("_id", (Object) morphiumId);

        Map<String, Object> query = Doc.of("_id", Doc.of("$in", List.of(morphiumId)));
        assertTrue(QueryHelper.matchesQuery(query, doc, null));

        query = Doc.of("_id", Doc.of("$in", List.of(new ObjectId(morphiumId.toString()))));
        assertTrue(QueryHelper.matchesQuery(query, doc, null));

        query = Doc.of("_id", Doc.of("$nin", List.of(new ObjectId(morphiumId.toString()))));
        assertFalse(QueryHelper.matchesQuery(query, doc, null));
    }

    @Test
    public void existsOnDottedPathTest() {
        Map<String, Object> doc = Doc.of("profile", Doc.of("email", null), "items", List.of(Doc.of("name", "foo")));

        Map<String, Object> query = Doc.of("profile.email", Doc.of("$exists", true));
        assertTrue(QueryHelper.matchesQuery(query, doc, null));

        query = Doc.of("profile.phone", Doc.of("$exists", true));
        assertFalse(QueryHelper.matchesQuery(query, doc, null));

        query = Doc.of("items.name", Doc.of("$exists", true));
        assertTrue(QueryHelper.matchesQuery(query, doc, null));

        query = Doc.of("items.age", Doc.of("$exists", true));
        assertFalse(QueryHelper.matchesQuery(query, doc, null));
    }

    @Test
    public void orMatchTest() throws Exception {
        Map<String, Object> doc = UtilsMap.of("counter", (Object) 12, "str_value", "hello");

        Query<UncachedObject> query = morphium.createQueryFor(UncachedObject.class);
        query.or(query.q().f("counter").eq(12), query.q().f("strValue").eq("not hello"));
        assertTrue(QueryHelper.matchesQuery(query.toQueryObject(), doc, null));

        query = morphium.createQueryFor(UncachedObject.class);
        query.or(query.q().f("strValue").eq("not hello"), query.q().f("counter").eq(12));
        assertTrue(QueryHelper.matchesQuery(query.toQueryObject(), doc, null));

        query = morphium.createQueryFor(UncachedObject.class);
        query.or(query.q().f("str_value").eq("not hello"), query.q().f("counter").eq(22));
        assertFalse(QueryHelper.matchesQuery(query.toQueryObject(), doc, null));
//        query=morphium.createQueryFor(UncachedObject.class).f("counter").eq(12)
//                .f("value").eq("hello").toQueryObject();
//
//        assert (QueryHelper.matchesQuery(query,doc));

    }
    @Test
    public void geoNearTests() throws Exception {
        GeoSearchTests.Place p = new GeoSearchTests.Place();
        p.setPosition(Arrays.asList(-73.9667,40.78));
        var ret=QueryHelper.matchesQuery(Doc.of("position",Doc.of("$near",Doc.of("$geometry",Doc.of("type","Point","coordinates",Arrays.asList(-73.9966,40.77)))),
          "$minDistance",1000,"$maxDistance",2000),
          morphium.getMapper().serialize(p), null);
        assertTrue(ret);
    }

}
