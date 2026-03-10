package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.AdditionalData;
import de.caluga.morphium.annotations.Index;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * User: Stephan Bösebeck
 * Date: 07.09.12
 * Time: 08:00
 * <p>
 */
@Tag("core")
public class SubDocumentTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testSubDocQuery(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(UncachedObject.class);
            morphium.dropCollection(ComplexObject.class);
            Thread.sleep(250);
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
            ref.setCounter(200);
            ref.setStrValue("The reference");
            morphium.store(ref);

            co.setRef(ref);
            co.setEinText("This is a very complex object");
            morphium.store(co);

            TestUtils.waitForWrites(morphium, log);
            Thread.sleep(1500);
            Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class);
            q = q.f("embed.value").eq("A value");
            List<ComplexObject> lst = q.asList();
            assertNotNull(lst);
            ;
            assertEquals(1, lst.size(), "List size wrong: " + lst.size());
            assertEquals(lst.get(0).getId(), co.getId());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testSubDocNoExistQuery(Morphium morphium) {
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

            TestUtils.waitForWrites(morphium, log);

            Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class);
            q = q.f("embed.value").eq("A value_not");
            List<ComplexObject> lst = q.asList();
            assertNotNull(lst);
            ;
            assertTrue(lst.isEmpty());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testSubDocAdditionals(Morphium morphium) throws Exception {
        try (morphium) {
            SubDocumentAdditional s = new SubDocumentAdditional();
            s.setStrValue("SubDoc");
            s.setCounter(102);
            s.additionals = new HashMap<>();
            s.additionals.put("test", 100);
            s.additionals.put("sub", UtilsMap.of("val", 42));

            morphium.store(s);
            Thread.sleep(100);
            List<SubDocumentAdditional> lst = morphium.createQueryFor(SubDocumentAdditional.class).f("sub.val").eq(42).asList();
            long st = System.currentTimeMillis();
            while (lst.size() != 1) {
                Thread.sleep(100);
                lst = morphium.createQueryFor(SubDocumentAdditional.class).f("sub.val").eq(42).asList();
                assert (System.currentTimeMillis() - st < 5000);
            }
            assertEquals(1, lst.size());
            assertNotNull(lst.get(0).additionals.get("sub"));
            ;
            assertTrue(lst.get(0).additionals.get("sub") instanceof Map);
        }
    }

    @Index({"embed.id"})
    public static class SubDocumentIndex extends ComplexObject {

    }


    public static class SubDocumentAdditional extends UncachedObject {
        @AdditionalData(readOnly = false)
        public Map<String, Object> additionals;
    }

}
