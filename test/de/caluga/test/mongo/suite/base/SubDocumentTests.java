package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.AdditionalData;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 07.09.12
 * Time: 08:00
 * <p/>
 */
public class SubDocumentTests extends MorphiumTestBase {

    @Test
    public void testSubDocQuery() throws Exception {
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

        waitForWrites();
        Thread.sleep(1500);
        Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class);
        q = q.f("embed.value").eq("A value");
        List<ComplexObject> lst = q.asList();
        assert (lst != null);
        assert (lst.size() == 1) : "List size wrong: " + lst.size();
        assert (lst.get(0).getId().equals(co.getId()));
    }

    @Test
    public void testSubDocNoExistQuery() {
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

        waitForWrites();

        Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class);
        q = q.f("embed.value").eq("A value_not");
        List<ComplexObject> lst = q.asList();
        assert (lst != null);
        assert (lst.isEmpty());
    }

    @Test
    public void testSubDocAdditionals() throws Exception {
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
        assert (lst.size() == 1);
        assert (lst.get(0).additionals.get("sub") != null);
        assert (lst.get(0).additionals.get("sub") instanceof Map);
    }


    @Test
    public void testSubDocumentIndex() {
        morphium.ensureIndicesFor(SubDocumentIndex.class);
    }

    @Index({"embed.id"})
    public static class SubDocumentIndex extends ComplexObject {

    }


    public static class SubDocumentAdditional extends UncachedObject {
        @AdditionalData(readOnly = false)
        public Map<String, Object> additionals;
    }

}
