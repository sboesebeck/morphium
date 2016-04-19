package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 07.09.12
 * Time: 08:00
 * <p/>
 */
public class SubDocumentTests extends MongoTest {

    @Test
    public void testSubDocQuery() throws Exception {
        UncachedObject o = new UncachedObject();
        o.setCounter(111);
        o.setValue("Embedded object");
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
        ref.setValue("The reference");
        morphium.store(ref);

        co.setRef(ref);
        co.setEinText("This is a very complex object");
        morphium.store(co);

        waitForWrites();

        Query<ComplexObject> q = morphium.createQueryFor(ComplexObject.class);
        q = q.f("embed.value").eq("A value");
        List<ComplexObject> lst = q.asList();
        assert (lst != null);
        assert (lst.size() == 1);
        assert (lst.get(0).getId().equals(co.getId()));
    }

    @Test
    public void testSubDocNoExistQuery() throws Exception {
        UncachedObject o = new UncachedObject();
        o.setCounter(111);
        o.setValue("Embedded object");
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
        ref.setValue("The reference");
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
    public void testSubDocumentIndex() throws Exception {
        morphium.ensureIndicesFor(SubDocumentIndex.class);
    }

    @Index({"embed.id"})
    public static class SubDocumentIndex extends ComplexObject {

    }

}
