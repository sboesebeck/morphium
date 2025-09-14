package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * User: Stpehan Bösebeck
 * Date: 29.03.12
 * Time: 15:56
 * <p>
 * testing compley queryies on Morphium
 */
@Tag("core")
public class ComplexTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testStoreAndRead(Morphium morphium) {
        try (morphium) {
            UncachedObject o = new UncachedObject();
            o.setCounter(111);
            o.setStrValue("Embedded object");
            //morphium.store(o);
            EmbeddedObject eo = new EmbeddedObject();
            eo.setName("Embedded object 1");
            eo.setValue("A value");
            eo.setTest(System.currentTimeMillis());
            ComplexObject co = new ComplexObject();
            co.setEmbed(eo);
            co.setEntityEmbeded(o);
            UncachedObject ref = new UncachedObject();
            ref.setCounter(100);
            ref.setStrValue("The reference");
            morphium.store(ref);
            co.setRef(ref);
            co.setEinText("This is a very complex object");
            morphium.store(co);
            //object stored!!!
            TestUtils.waitForWrites(morphium, log);
            //now read it again...
            Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class);
            ComplexObject co2 = q.getById(co.getId());
            log.info("Just loaded: " + co2.toString());
            log.info("Stored     : " + co);
            assert(co2.getId().equals(co.getId())) : "Ids not equal?";
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testAccessTimestamps(Morphium morphium) throws Exception {
        try (morphium) {
            log.info("-------> running test with: " + morphium.getDriver().getName());
            ComplexObject o = new ComplexObject();
            o.setEinText("A test");
            o.setTrans("Tansient");
            o.setNullValue(15);
            //And test for null-References!
            morphium.store(o);
            assert(o.getChanged() != 0) : "Last change not set!?!?";
            Thread.sleep(150);
            Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class).f("ein_text").eq("A test");
            o = q.get();
            assert(o.getLastAccess() != 0) : "Last access not set!";
            o = new ComplexObject();
            o.setEinText("A test2");
            o.setTrans("Tansient");
            o.setNullValue(18);
            List<ComplexObject> lst = morphium.readAll(ComplexObject.class);

            for (ComplexObject co : lst) {
                assert(co.getChanged() != 0) : "Last Access not set!";
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testCopmplexQuery(Morphium morphium) throws Exception {
        try (morphium) {
            for (int i = 1; i <= 100; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Uncached " + i);
                morphium.store(o);
            }

            Thread.sleep(100);
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q.f("counter").lt(50).or(q.q().f("counter").eq(10), q.q().f("str_value").eq("Uncached 15"));
            List<UncachedObject> lst = q.asList();
            assert(lst.size() == 2) : "List size wrong: " + lst.size();

            for (UncachedObject o : lst) {
                assert(o.getCounter() < 50 && (o.getCounter() == 10 || o.getCounter() == 15)) : "Counter wrong: " + o.getCounter();
            }

            q = morphium.createQueryFor(UncachedObject.class);
            q.f("counter").lt(50).or(q.q().f("counter").eq(10), q.q().f("strValue").eq("Uncached 15"), q.q().f("counter").eq(52));
            lst = q.asList();
            assert(lst.size() == 2) : "List size wrong: " + lst.size();

            for (UncachedObject o : lst) {
                assert(o.getCounter() < 50 && (o.getCounter() == 10 || o.getCounter() == 15)) : "Counter wrong: " + o.getCounter();
            }

            q = morphium.createQueryFor(UncachedObject.class);
            q.f("counter").lt(50).f("counter").gt(10).or(q.q().f("counter").eq(22), q.q().f("str_value").eq("Uncached 15"), q.q().f("counter").gte(70));
            lst = q.asList();
            assert(lst.size() == 2) : "List size wrong: " + lst.size();

            for (UncachedObject o : lst) {
                assert(o.getCounter() < 50 && o.getCounter() > 10 && (o.getCounter() == 22 || o.getCounter() == 15)) : "Counter wrong: " + o.getCounter();
            }
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testNorQuery(Morphium morphium) throws Exception {
        try (morphium) {
            for (int i = 1; i <= 100; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Uncached " + i);
                morphium.store(o);
            }

            Thread.sleep(500);
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q.nor(q.q().f("counter").lt(90), q.q().f("counter").gt(95));
            log.info("Query: " + q.toQueryObject().toString());
            List<UncachedObject> lst = q.asList();
            assert(lst.size() == 6) : "List size wrong: " + lst.size();

            for (UncachedObject o : lst) {
                assert(!(o.getCounter() < 90 || o.getCounter() > 95)) : "Counter wrong: " + o.getCounter();
            }
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void complexQuery(Morphium morphium) throws Exception {
        try (morphium) {
            for (int i = 1; i <= 100; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Uncached " + i);
                morphium.store(o);
            }

            Thread.sleep(250);
            Map<String, Object> query = new HashMap<>();
            query.put("counter", UtilsMap.of("$lt", 10));
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            List<UncachedObject> lst = q.rawQuery(query).asList();
            assert(lst != null && !lst.isEmpty()) : "Nothing found?";
            assert(lst.size() == 9);

            for (UncachedObject o : lst) {
                assert(o.getCounter() < 10) : "Wrong counter: " + o.getCounter();
            }

            //test for iterator
            int cnt = 0;

            for (UncachedObject o : q.asIterable()) {
                assert(o.getCounter() < 10) : "Wrong counter: " + o.getCounter();
                cnt++;
            }

            assert(cnt == 9);
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void referenceQuery(Morphium morphium) throws Exception {
        try (morphium) {
            UncachedObject o = new UncachedObject();
            o.setCounter(15);
            o.setStrValue("Uncached " + 15);
            morphium.store(o);
            ComplexObject co = new ComplexObject();
            co.setEinText("Text");
            co.setRef(o);
            co.setTrans("trans");
            morphium.store(co);
            Thread.sleep(500);
            Query<ComplexObject> qc = morphium.createQueryFor(ComplexObject.class);
            qc.f("ref").eq(o);
            ComplexObject fnd = qc.get();
            assertNotNull(fnd, "not found?!?!");
            assert(fnd.getEinText().equals(co.getEinText())) : "Text different?";
            assert(fnd.getRef().getCounter() == co.getRef().getCounter()) : "Reference broken?";
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void searchForSubObj(Morphium morphium) throws Exception {
        try (morphium) {
            UncachedObject o = new UncachedObject();
            o.setCounter(15);
            o.setStrValue("Uncached " + 15);
            morphium.store(o);
            ComplexObject co = new ComplexObject();
            co.setEinText("Text");
            co.setRef(o);
            co.setTrans("trans");
            EmbeddedObject eo = new EmbeddedObject();
            eo.setName("embedded1");
            eo.setValue("154");
            co.setEmbed(eo);
            morphium.store(co);
            TestUtils.waitForWrites(morphium, log);
            Thread.sleep(1000);
            Query<ComplexObject> qc = morphium.createQueryFor(ComplexObject.class);
            co = qc.f("embed.name").eq("embedded1").get();
            assertNotNull(co);
            ;
            assertNotNull(co.getEmbed());
            ;
            assert(co.getEmbed().getName().equals("embedded1"));
            assert(co.getEinText().equals("Text"));
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void complexQueryCallTest(Morphium morphium) throws Exception {
        try (morphium) {
            createUncachedObjects(morphium, 100);
            Thread.sleep(100);
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            UncachedObject uc = q.rawQuery(UtilsMap.of("counter", 10)).asList().get(0);
            assert(uc.getCounter() == 10);
            assert(q.q().rawQuery(UtilsMap.of("counter", UtilsMap.of("$lte", 50))).countAll() == 50);
            assert(q.q().rawQuery(UtilsMap.of("counter", UtilsMap.of("$lte", 50))).asList().size() == 50);
        }
    }
}
