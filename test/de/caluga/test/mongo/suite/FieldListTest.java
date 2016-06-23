package de.caluga.test.mongo.suite;

import de.caluga.morphium.Utils;
import de.caluga.morphium.annotations.ReadOnly;
import de.caluga.morphium.annotations.WriteOnly;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
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

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.addProjection(UncachedObject.Fields.counter);

        q = q.f(UncachedObject.Fields.counter).eq(30);
        UncachedObject uc = q.get();
        assert (uc.getValue() == null) : "Value is " + uc.getValue();
    }

    @Test
    public void testWriteOnly() throws Exception {
        morphium.dropCollection(WriteOnlyObject.class);
        WriteOnlyObject wo = new WriteOnlyObject();
        wo.setCounter(101);
        wo.setValue("a value");
        wo.writeOnlyValue = "write only";

        morphium.store(wo);
        morphium.reread(wo);
        assert (wo.writeOnlyValue == null) : "read from db: " + wo.writeOnlyValue;


        List<Map<String, Object>> lst = morphium.getDriver().find(morphium.getConfig().getDatabase(), "write_only_object",
                Utils.getMap("_id", wo.getMorphiumId()), null, null, 0, 1000, 1000, null, null);
        Map<String, Object> obj = lst.get(0);
        assert (obj.get("write_only_value").equals("write only"));

    }

    @Test
    public void testReadOnly() throws Exception {
        morphium.dropCollection(ReadOnlyObject.class);
        ReadOnlyObject ro = new ReadOnlyObject();
        ro.setValue("ReadOnlyTest");
        ro.setCounter(100);
        ro.readOnlyValue = "Must not be stored!";

        morphium.store(ro);
        morphium.reread(ro);

        assert (ro.readOnlyValue == null);

        ro.setValue("OtherValue");
        ro.readOnlyValue = "must still not be stored, even after update!";
        morphium.store(ro);
        morphium.reread(ro);
        assert (ro.readOnlyValue == null);

        //forcing store of a value
        Map<String, Object> marshall = morphium.getMapper().marshall(ro);
        marshall.put("read_only_value", "stored in db");
        List<Map<String, Object>> lst = new ArrayList<>();
        lst.add(marshall);
        morphium.getDriver().store(morphium.getConfig().getDatabase(), "read_only_object", lst, null);

        morphium.reread(ro);
        assert (ro.readOnlyValue.equals("stored in db"));

        ro.readOnlyValue = "different";
        morphium.reread(ro);
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
