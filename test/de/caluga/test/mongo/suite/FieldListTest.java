package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.ReadOnly;
import de.caluga.morphium.annotations.WriteOnly;
import de.caluga.morphium.query.Query;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 25.04.14
 * Time: 11:43
 * To change this template use File | Settings | File Templates.
 */
public class FieldListTest extends MongoTest {

    @Test
    public void testFieldList() throws Exception {
        createUncachedObjects(100);
        waitForWrites();

        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q.addReturnedField(UncachedObject.Fields.counter);

        q = q.f(UncachedObject.Fields.counter).eq(30);
        UncachedObject uc = q.get();
        assert (uc.getValue() == null) : "Value is " + uc.getValue();
    }

    @Test
    public void testWriteOnly() throws Exception {
        MorphiumSingleton.get().dropCollection(WriteOnlyObject.class);
        WriteOnlyObject wo = new WriteOnlyObject();
        wo.setCounter(101);
        wo.setValue("a value");
        wo.writeOnlyValue = "write only";

        MorphiumSingleton.get().store(wo);
        MorphiumSingleton.get().reread(wo);
        assert (wo.writeOnlyValue == null) : "read from db: " + wo.writeOnlyValue;


        List<Map<String, Object>> lst = MorphiumSingleton.get().getDriver().find(MorphiumSingleton.get().getConfig().getDatabase(), "write_only_object",
                MorphiumSingleton.get().getMap("_id", wo.getMongoId()), null, null, 0, 1000, 1000, null, null);
        Map<String, Object> obj = lst.get(0);
        assert (obj.get("write_only_value").equals("write only"));

    }

    @Test
    public void testReadOnly() throws Exception {
        MorphiumSingleton.get().dropCollection(ReadOnlyObject.class);
        ReadOnlyObject ro = new ReadOnlyObject();
        ro.setValue("ReadOnlyTest");
        ro.setCounter(100);
        ro.readOnlyValue = "Must not be stored!";

        MorphiumSingleton.get().store(ro);
        MorphiumSingleton.get().reread(ro);

        assert (ro.readOnlyValue == null);

        ro.setValue("OtherValue");
        ro.readOnlyValue = "must still not be stored, even after update!";
        MorphiumSingleton.get().store(ro);
        MorphiumSingleton.get().reread(ro);
        assert (ro.readOnlyValue == null);

        //forcing store of a value
        Map<String, Object> marshall = MorphiumSingleton.get().getMapper().marshall(ro);
        marshall.put("read_only_value", "stored in db");
        List<Map<String, Object>> lst = new ArrayList<>();
        lst.add(marshall);
        MorphiumSingleton.get().getDriver().insert(MorphiumSingleton.get().getConfig().getDatabase(), "read_only_object", lst, null);

        MorphiumSingleton.get().reread(ro);
        assert (ro.readOnlyValue.equals("stored in db"));

        ro.readOnlyValue = "different";
        MorphiumSingleton.get().reread(ro);
        assert (ro.readOnlyValue.equals("stored in db"));

    }


    public static class WriteOnlyObject extends UncachedObject {
        @WriteOnly
        private String writeOnlyValue;
    }

    public static class ReadOnlyObject extends UncachedObject {
        @ReadOnly
        private String readOnlyValue;
    }

}
