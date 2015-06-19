package de.caluga.test.mongo.suite;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.ReadOnly;
import de.caluga.morphium.annotations.WriteOnly;
import de.caluga.morphium.query.Query;
import org.junit.Test;

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

        DBCursor cursor = MorphiumSingleton.get().getDatabase().getCollection("write_only_object").find(new BasicDBObject("_id", wo.getMongoId()));
        DBObject obj = cursor.next();
        cursor.close();
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
        DBObject marshall = MorphiumSingleton.get().getMapper().marshall(ro);
        marshall.put("read_only_value", "stored in db");
        MorphiumSingleton.get().getDatabase().getCollection("read_only_object").save(marshall);

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
